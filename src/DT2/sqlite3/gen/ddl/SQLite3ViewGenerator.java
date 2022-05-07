package DT2.sqlite3.gen.ddl;

import java.sql.SQLException;

import DT2.Randomly;
import DT2.common.DBMSCommon;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.sqlite3.SQLite3Errors;
import DT2.sqlite3.SQLite3GlobalState;
import DT2.sqlite3.SQLite3Options.SQLite3OracleFactory;
import DT2.sqlite3.SQLite3Visitor;
import DT2.sqlite3.ast.SQLite3Expression;
import DT2.sqlite3.ast.SQLite3Select;
import DT2.sqlite3.gen.SQLite3Common;
import DT2.sqlite3.oracle.SQLite3RandomQuerySynthesizer;
import DT2.sqlite3.schema.SQLite3Schema;

public final class SQLite3ViewGenerator {

    private SQLite3ViewGenerator() {
    }

    public static SQLQueryAdapter dropView(SQLite3GlobalState globalState) {
        SQLite3Schema s = globalState.getSchema();
        StringBuilder sb = new StringBuilder("DROP VIEW ");
        sb.append(s.getRandomViewOrBailout().getName());
        return new SQLQueryAdapter(sb.toString(), true);
    }

    public static SQLQueryAdapter generate(SQLite3GlobalState globalState) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("TEMP", "TEMPORARY"));
        }
        sb.append(" VIEW ");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS ");
        }
        sb.append(SQLite3Common.getFreeViewName(globalState.getSchema()));
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        errors.add("is circularly defined");
        errors.add("unsupported frame specification");
        errors.add("The database file is locked");
        int size = 1 + Randomly.smallNumber();
        columnNamesAs(sb, size);
        SQLite3Expression randomQuery;
        do {
            randomQuery = SQLite3RandomQuerySynthesizer.generate(globalState, size);
        } while (globalState.getDbmsSpecificOptions().oracles == SQLite3OracleFactory.PQS
                && !checkAffinity(randomQuery));
        sb.append(SQLite3Visitor.asString(randomQuery));
        return new SQLQueryAdapter(sb.toString(), errors, true);

    }

    /**
     * The affinity of columns in a view cannot be determined using features of the DBMS - this would need to be parsed
     * from the CREATE TABLE and CREATE VIEW statements. This is non-trivial, and currently not implemented. Rather, we
     * avoid generating expressions with an affinity or view.
     *
     * @see http://sqlite.1065341.n5.nabble.com/Determining-column-collating-functions-td108157.html#a108159
     *
     * @param randomQuery
     *
     * @return true if the query can be used for PQS
     */
    private static boolean checkAffinity(SQLite3Expression randomQuery) {
        if (randomQuery instanceof SQLite3Select) {
            for (SQLite3Expression expr : ((SQLite3Select) randomQuery).getFetchColumns()) {
                if (expr.getExpectedValue() == null || expr.getAffinity() != null
                        || expr.getImplicitCollateSequence() != null || expr.getExplicitCollateSequence() != null) {
                    return false;
                }
            }
            return true;
        } else {
            return false; // the columns in UNION clauses can also have affinities
        }
    }

    private static void columnNamesAs(StringBuilder sb, int size) {
        sb.append("(");
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(DBMSCommon.createColumnName(i));
        }
        sb.append(")");
        sb.append(" AS ");
    }

}
