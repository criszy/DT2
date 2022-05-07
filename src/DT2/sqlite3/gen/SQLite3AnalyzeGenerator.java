package DT2.sqlite3.gen;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.sqlite3.SQLite3GlobalState;

public final class SQLite3AnalyzeGenerator {

    private SQLite3AnalyzeGenerator() {
    }

    private enum AnalyzeTarget {
        SCHEMA, TABLE, INDEX, SQL_MASTER
    }

    public static SQLQueryAdapter generateAnalyze(SQLite3GlobalState globalState) {
        final StringBuilder sb = new StringBuilder("ANALYZE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            switch (Randomly.fromOptions(AnalyzeTarget.values())) {
            case INDEX:
                sb.append(globalState.getSchema().getRandomIndexOrBailout());
                break;
            case SCHEMA:
                sb.append(Randomly.fromOptions("main", "temp"));
                break;
            case SQL_MASTER:
                sb.append("sqlite_master");
                break;
            case TABLE:
                sb.append(globalState.getSchema().getRandomTableOrBailout().getName());
                break;
            default:
                throw new AssertionError();
            }
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("The database file is locked"));
    }

}
