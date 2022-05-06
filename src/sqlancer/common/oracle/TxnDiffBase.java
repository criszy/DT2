package sqlancer.common.oracle;

import sqlancer.*;
import sqlancer.common.oracle.txndiff.DiffTool;
import sqlancer.common.oracle.txndiff.StatementCell;
import sqlancer.common.oracle.txndiff.Transaction;
import sqlancer.tidb.oracle.TiDBTxnDiffOracle;

import java.util.ArrayList;
import java.util.Random;

public abstract class TxnDiffBase<S extends SQLGlobalState<?, ?>>
        implements TestOracle {
    protected final S state;
    protected final MainOptions options;
    protected final Main.StateLogger logger;
    protected boolean testTiDB;
    protected boolean testMariaDB;

    public TxnDiffBase(S state) {
        this.state = state;
        this.options = state.getOptions();
        this.logger = state.getLogger();
        this.testTiDB = state.getOptions().isTestTiDB();
        this.testMariaDB = state.getOptions().isTestMariaDB();
    }

    protected abstract String genSelectStatement();

    protected abstract String genInsertStatement();

    protected abstract String genUpdateStatement();

    protected abstract String genDeleteStatement();

    public Transaction genTransaction(int txId) {
        Transaction txn = new Transaction(txId);
        int n = Randomly.getNextInt(DiffTool.TxnSizeMin, DiffTool.TxnSizeMax);
        ArrayList<StatementCell> statementList = new ArrayList<>();
        StatementCell cell = new StatementCell(txn, 0, "BEGIN");
        statementList.add(cell);
        for (int i = 1; i <= n; i++) {
            cell = new StatementCell(txn, i, genStatement());
            statementList.add(cell);
        }
        String lastStmt = "COMMIT";
        if (Randomly.getBoolean()) {
            lastStmt = "ROLLBACK";
        }
        cell = new StatementCell(txn, n+1, lastStmt);
        statementList.add(cell);
        txn.setStatements(statementList);
        return txn;
    }

    public String genStatement() {
        String statement;
        int random = new Random().nextInt(9);
        switch (random) {
            case 0:
            case 1:
            case 2:
                statement = genSelectStatement();
                break;
            case 3:
                statement = genInsertStatement();
                break;
            case 4:
            case 5:
            case 6:
            case 7:
                statement = genUpdateStatement();
                break;
            case 8:
                statement = genDeleteStatement();
                break;
            default:
                statement = genUpdateStatement();
        }
        return statement;
    }

}
