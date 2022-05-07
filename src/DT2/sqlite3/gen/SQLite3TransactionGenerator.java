package DT2.sqlite3.gen;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.sqlite3.SQLite3GlobalState;

public final class SQLite3TransactionGenerator {

    private SQLite3TransactionGenerator() {
    }

    public static SQLQueryAdapter generateCommit(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append(Randomly.fromOptions("COMMIT", "END"));
        if (Randomly.getBoolean()) {
            sb.append(" TRANSACTION");
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("no transaction is active",
                "The database file is locked", "FOREIGN KEY constraint failed"), true);
    }

    public static SQLQueryAdapter generateBeginTransaction(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("BEGIN ");
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE", "EXCLUSIVE"));
        }
        sb.append(" TRANSACTION;");
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("cannot start a transaction within a transaction", "The database file is locked"));
    }

    public static SQLQueryAdapter generateRollbackTransaction(SQLite3GlobalState globalState) {
        // TODO: could be extended by savepoint
        return new SQLQueryAdapter("ROLLBACK TRANSACTION;",
                ExpectedErrors.from("no transaction is active", "The database file is locked"), true);
    }

}
