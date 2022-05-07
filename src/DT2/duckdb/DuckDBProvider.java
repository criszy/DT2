package DT2.duckdb;

import java.sql.DriverManager;
import java.sql.SQLException;

import com.google.auto.service.AutoService;

import DT2.AbstractAction;
import DT2.DatabaseProvider;
import DT2.IgnoreMeException;
import DT2.Randomly;
import DT2.SQLConnection;
import DT2.SQLGlobalState;
import DT2.SQLProviderAdapter;
import DT2.StatementExecutor;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.query.SQLQueryProvider;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.gen.DuckDBDeleteGenerator;
import DT2.duckdb.gen.DuckDBIndexGenerator;
import DT2.duckdb.gen.DuckDBInsertGenerator;
import DT2.duckdb.gen.DuckDBRandomQuerySynthesizer;
import DT2.duckdb.gen.DuckDBTableGenerator;
import DT2.duckdb.gen.DuckDBUpdateGenerator;
import DT2.duckdb.gen.DuckDBViewGenerator;

@AutoService(DatabaseProvider.class)
public class DuckDBProvider extends SQLProviderAdapter<DuckDBGlobalState, DuckDBOptions> {

    public DuckDBProvider() {
        super(DuckDBGlobalState.class, DuckDBOptions.class);
    }

    public enum Action implements AbstractAction<DuckDBGlobalState> {

        INSERT(DuckDBInsertGenerator::getQuery), //
        CREATE_INDEX(DuckDBIndexGenerator::getQuery), //
        VACUUM((g) -> new SQLQueryAdapter("VACUUM;")), //
        ANALYZE((g) -> new SQLQueryAdapter("ANALYZE;")), //
        DELETE(DuckDBDeleteGenerator::generate), //
        UPDATE(DuckDBUpdateGenerator::getQuery), //
        CREATE_VIEW(DuckDBViewGenerator::generate), //
        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            DuckDBErrors.addExpressionErrors(errors);
            DuckDBErrors.addGroupByErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + DuckDBToStringVisitor
                            .asString(DuckDBRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final SQLQueryProvider<DuckDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<DuckDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(DuckDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(DuckDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            if (!globalState.getDbmsSpecificOptions().testIndexes) {
                return 0;
            }
            // fall through
        case UPDATE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case VACUUM: // seems to be ignored
        case ANALYZE: // seems to be ignored
        case EXPLAIN:
            return r.getInteger(0, 2);
        case DELETE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        case CREATE_VIEW:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class DuckDBGlobalState extends SQLGlobalState<DuckDBOptions, DuckDBSchema> {

        @Override
        protected DuckDBSchema readSchema() throws SQLException {
            return DuckDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(DuckDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new DuckDBTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<DuckDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                DuckDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(DuckDBGlobalState globalState) throws SQLException {
        String url = "jdbc:duckdb:";
        return new SQLConnection(DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword()));
    }

    @Override
    public String getDBMSName() {
        return "duckdb";
    }

}
