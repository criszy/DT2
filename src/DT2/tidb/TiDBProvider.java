package DT2.tidb;

import java.sql.*;

import com.google.auto.service.AutoService;

import DT2.AbstractAction;
import DT2.DatabaseProvider;
import DT2.IgnoreMeException;
import DT2.MainOptions;
import DT2.Randomly;
import DT2.SQLConnection;
import DT2.SQLGlobalState;
import DT2.SQLProviderAdapter;
import DT2.StatementExecutor;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.query.SQLQueryProvider;
import DT2.tidb.TiDBProvider.TiDBGlobalState;
import DT2.tidb.gen.TiDBIndexGenerator;
import DT2.tidb.gen.TiDBInsertGenerator;
import DT2.tidb.gen.TiDBTableGenerator;

@AutoService(DatabaseProvider.class)
public class TiDBProvider extends SQLProviderAdapter<TiDBGlobalState, TiDBOptions> {

    public TiDBProvider() {
        super(TiDBGlobalState.class, TiDBOptions.class);
    }

    public enum Action implements AbstractAction<TiDBGlobalState> {
        INSERT(TiDBInsertGenerator::getQuery), //
//        ANALYZE_TABLE(TiDBAnalyzeTableGenerator::getQuery), //
//        TRUNCATE((g) -> new SQLQueryAdapter("TRUNCATE " + g.getSchema().getRandomTable(t -> !t.isView()).getName())), //
        CREATE_INDEX(TiDBIndexGenerator::getQuery), //
//        DELETE(TiDBDeleteGenerator::getQuery), //
//        SET(TiDBSetGenerator::getQuery), //
//        UPDATE(TiDBUpdateGenerator::getQuery), //
//        ADMIN_CHECKSUM_TABLE(
//                (g) -> new SQLQueryAdapter("ADMIN CHECKSUM TABLE " + g.getSchema().getRandomTable().getName())), //
//        VIEW_GENERATOR(TiDBViewGenerator::getQuery), //
//        ALTER_TABLE(TiDBAlterTableGenerator::getQuery), //
//        EXPLAIN((g) -> {
//            ExpectedErrors errors = new ExpectedErrors();
//            TiDBErrors.addExpressionErrors(errors);
//            TiDBErrors.addExpressionHavingErrors(errors);
//            return new SQLQueryAdapter(
//                    "EXPLAIN " + TiDBRandomQuerySynthesizer.generate(g, Randomly.smallNumber() + 1).getQueryString(),
//                    errors);
//        });
        ;

        private final SQLQueryProvider<TiDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<TiDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(TiDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class TiDBGlobalState extends SQLGlobalState<TiDBOptions, TiDBSchema> {

        @Override
        protected TiDBSchema readSchema() throws SQLException {
            return TiDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    private static int mapActions(TiDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
//        case ANALYZE_TABLE:
        case CREATE_INDEX:
            return r.getInteger(0, 2);
        case INSERT:
//        case EXPLAIN:
            return r.getInteger(3, globalState.getOptions().getMaxNumberInserts());
//        case TRUNCATE:
//        case DELETE:
//        case ADMIN_CHECKSUM_TABLE:
//            return r.getInteger(0, 2);
//        case SET:
//        case UPDATE:
//            return r.getInteger(0, 5);
//        case VIEW_GENERATOR:
//            // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/8
//            return r.getInteger(0, 2);
//        case ALTER_TABLE:
//            return r.getInteger(0, 10); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/10
        default:
            throw new AssertionError(a);
        }

    }

    @Override
    public void generateDatabase(TiDBGlobalState globalState) throws Exception {
        System.out.println("test");
        dropExistTable(globalState);

        if (globalState.getOptions().isSetCase()) {
            return;
        }

        for (int i = 0; i < Randomly.fromOptions(1, 3); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new TiDBTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
//        boolean success;
//        do {
//            SQLQueryAdapter qt = new TiDBTableGenerator().getQuery(globalState);
//            success = globalState.executeStatement(qt);
//        } while (!success);

        StatementExecutor<TiDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                TiDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        try {
            se.executeStatements();
        } catch (SQLException e) {
            if (e.getMessage().contains(
                    "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")) {
                throw new IgnoreMeException(); // TODO: drop view instead
            } else {
                throw new AssertionError(e);
            }
        }
    }

    @Override
    public SQLConnection createDatabase(TiDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = TiDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = TiDBOptions.DEFAULT_PORT;
        }

//        String databaseName = globalState.getDatabaseName();
        Connection con;
        String databaseName = globalState.getOptions().getDbName();
        String url = String.format("jdbc:mysql://%s:%d/%s", host, port, databaseName);
        try{
            con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                    globalState.getOptions().getPassword());
//            globalState.getState().logStatement("USE " + databaseName);
        } catch (Exception e) {
            throw new RuntimeException("failed to connect to database");
        }

//        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
//        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
//        globalState.getState().logStatement(createDatabaseCommand);
//        globalState.getState().logStatement("USE " + databaseName);
//        try (Statement s = con.createStatement()) {
//            s.execute("DROP DATABASE IF EXISTS " + databaseName);
//        }
//        try (Statement s = con.createStatement()) {
//            s.execute(createDatabaseCommand);
//        }
//        con.close();
//        con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:4000/" + databaseName,
//                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "tidb";
    }

    protected void dropExistTable(TiDBGlobalState globalState) throws SQLException {
        System.out.println(" -- clear database");
        try (Statement s = globalState.getConnection().createStatement()) {
            ResultSet tableRs = s.executeQuery("SHOW TABLES");
            while (tableRs.next()) {
                String tableName = tableRs.getString(1);
                try (Statement d = globalState.getConnection().createStatement()) {
                    d.execute("DROP TABLE IF EXISTS " + tableName);
                }
                System.out.println(" -- drop table " + tableName);
            }
            tableRs.close();
        }
    }

}
