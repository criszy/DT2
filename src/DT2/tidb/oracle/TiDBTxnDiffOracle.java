package DT2.tidb.oracle;

import DT2.Main;
import DT2.Randomly;
import DT2.common.oracle.TxnDiffBase;
import DT2.common.oracle.txndiff.*;
import DT2.common.query.Query;
import DT2.tidb.TiDBExpressionGenerator;
import DT2.tidb.TiDBProvider;
import DT2.tidb.TiDBSchema;
import DT2.tidb.TiDBSchema.TiDBTables;
import DT2.tidb.ast.*;
import DT2.tidb.gen.TiDBDeleteGenerator;
import DT2.tidb.gen.TiDBHintGenerator;
import DT2.tidb.gen.TiDBInsertGenerator;
import DT2.tidb.gen.TiDBUpdateGenerator;
import DT2.tidb.visitor.TiDBVisitor;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class TiDBTxnDiffOracle extends TxnDiffBase<TiDBProvider.TiDBGlobalState> {

    TiDBTables targetTables;

    public TiDBTxnDiffOracle(TiDBProvider.TiDBGlobalState state) {
        super(state);

    }

    @Override
    public void check() throws Exception {
        logger.writeCurrent(String.format("Run tests for %s in [DB %s] on [%s:%d]",
                options.getDbmsName(), options.getDbName(), options.getHost(), options.getPort()));
        System.out.println(String.format("Run tests for %s in [DB %s] on [%s:%d]",
                options.getDbmsName(), options.getDbName(), options.getHost(), options.getPort()));
        if (options.isTestMySQL()) {
            logger.writeCurrent(String.format("Run tests for MYSQL in [DB %s] on [%s:%d]",
                    options.getDbName(), options.getHost(), options.getMysqlPort()));
            System.out.println(String.format("Run tests for MYSQL in [DB %s] on [%s:%d]",
                    options.getDbName(), options.getHost(), options.getMysqlPort()));
        }
        if (options.isTestTiDB()) {
            logger.writeCurrent(String.format("Run tests for TIDB in [DB %s] on [%s:%d]",
                    options.getDbName(), options.getHost(), options.getTidbPort()));
            System.out.println(String.format("Run tests for TIDB in [DB %s] on [%s:%d]",
                    options.getDbName(), options.getHost(), options.getTidbPort()));
        }
        if (options.isTestMariaDB()) {
            logger.writeCurrent(String.format("Run tests for MARIADB in [DB %s] on [%s:%d]",
                    options.getDbName(), options.getHost(), options.getMariadbPort()));
            System.out.println(String.format("Run tests for MARIADB in [DB %s] on [%s:%d]",
                    options.getDbName(), options.getHost(), options.getMariadbPort()));
        }
        DiffTool.initialize(options);

        // build DBMSs connection
        DiffTool.genConnection();
        HashMap<Integer, Transaction> txns = new HashMap<>();
        if (options.isSetCase()) {
            Scanner scanner;
            if (options.getCaseFile().equals("")) {
                System.out.println("Read database and transaction from command line");
                scanner = new Scanner(System.in);
            } else {
                try {
                    File caseFile = new File(options.getCaseFile());
                    scanner = new Scanner(caseFile);
                    System.out.println("Read database and transaction from file: " + options.getCaseFile());
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Read case from file failed: ", e);
                }
            }
            DiffTool.prepareTableFromScanner(scanner);
            DiffTool.setTableNames();
            System.out.println("Initial table:\n" + DiffTool.initialTables());
            for (int i = 1; i <= options.getTxnNum(); i++) {
                txns.put(i, DiffTool.readTransactionFromScanner(scanner, i));
            }
            String scheduleStr = DiffTool.readScheduleFromScanner(scanner);
            scanner.close();
            System.out.println("Read transactions from file:\n");
            for (int i = 1; i <= options.getTxnNum(); i++) {
                System.out.println(txns.get(i));
            }
            DiffTool.setAllIsolationLevels();
            DiffChecker checker = new DiffChecker(txns);
            if (!scheduleStr.equals("")) {
                System.out.println("Get schedule from file: " + scheduleStr);
                checker.checkSchedule(scheduleStr, state);
            } else {
                checker.checkAll(state);
            }
            System.exit(0);
        } else {
            targetTables = state.getSchema().getRandomTableNonEmptyTables();
            int count = 0;
            while (true) {
                logger.writeCurrent("\n============================= Create new table =============================");
                System.out.println("\n============================= Create new table =============================");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {}

                // fetch table generation statements
                ReproduceState.statements.clear();
                List<Query<?>> stmts = state.getState().getStatements();
                for (Query<?> query : stmts) {
                    String statement = query.getQueryString();
                    ReproduceState.logStatement(statement);
                    logger.writeCurrent(statement);
                    System.out.println(statement);
                }

                DiffTool.bugReport.setInitializeStatements(ReproduceState.getStatements());

                DiffTool.setTableNames();

                // generate tables on other DBMSs
                if (!DiffTool.reproduceDBMS()) {
                    System.out.println(" -- Fail to reproduce databases");
                    break;
                }
                String initialTables = DiffTool.initialTables();
                DiffTool.bugReport.setInitialTable(initialTables);
                while (true) {
                    logger.writeCurrent("\n============================= Generate new transaction pair =============================");
                    System.out.println("\n============================= Generate new transaction pair =============================");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {}
                    initialTables = DiffTool.initialTables();
                    logger.writeCurrent(initialTables);
                    System.out.println(initialTables);
                    long startTime = System.currentTimeMillis();
                    for (int i = 1; i <= DiffTool.txnNum; i++) {
                        txns.put(i, genTransaction(i));
                        logger.writeCurrent(txns.get(i).toString());
                        System.out.println(txns.get(i).toString());
                    }
                    DiffTool.bugReport.setTxns(txns);
                    DiffTool.setAllIsolationLevels();
                    DiffChecker checker = new DiffChecker(txns);
                    checker.checkSeveral(state);
                    long endTime = System.currentTimeMillis() - startTime;
                    count++;
                    Main.testCases++;
                    StringBuilder sb = new StringBuilder();
                    sb.append("============================= Overall execution statistics =============================\n");
                    sb.append("transaction group: " + count + "\n");
                    sb.append("Total test case:" + Main.testCases + "\n");
                    sb.append("Total bug case:" + Main.bugCases + "\n");
                    sb.append("Time: " + endTime + "ms");
                    state.getLogger().writeCurrent(sb.toString());
                    System.out.println(sb.toString());
                    int choice = Randomly.getNextInt(0, 3);
                    if (choice == 1) {
                        break;
                    }
                    DiffTool.reproduceDBMS();
                    DiffTool.reproduceTables(DiffTool.conn);
                }
                int choice = Randomly.getNextInt(0, 3);
                if (choice == 1) {
                    break;
                }
            }
        }
    }

    @Override
    public String genSelectStatement(){
        TiDBSelect selectStatement = new TiDBSelect();
        List<TiDBExpression> fetchColumns = Arrays.asList(new TiDBColumnReference(targetTables.getColumns().get(0)));
        selectStatement.setFetchColumns(fetchColumns);
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        List<TiDBSchema.TiDBTable> tables = targetTables.getTables();
        if (Randomly.getBoolean()) {
            TiDBHintGenerator.generateHints(selectStatement, tables);
        }
        List<TiDBExpression> tableList = tables.stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        List<TiDBExpression> joins = TiDBJoin.getJoins(tableList, state);
        selectStatement.setJoinList(joins);
        selectStatement.setFromList(tableList);
        selectStatement.setWhereClause(null);
        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            selectStatement.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            selectStatement.setWhereClause(gen.generatePredicate());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectStatement.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        String queryString = TiDBVisitor.asString(selectStatement);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                queryString += " FOR SHARE";
            } else {
                queryString += " FOR UPDATE";
            }
        }
        return queryString;
    }

    @Override
    protected String genInsertStatement() {
        String insertString = null;
        try {
            insertString = TiDBInsertGenerator.getQuery(state).getQueryString();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return insertString;
    }

    @Override
    protected String genUpdateStatement() {
        String updateString = null;
        try {
            updateString = TiDBUpdateGenerator.getQuery(state).getQueryString();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return updateString;
    }

    @Override
    protected String genDeleteStatement() {
        String deleteString = null;
        try {
            deleteString = TiDBDeleteGenerator.getQuery(state).getQueryString();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return deleteString;
    }
}
