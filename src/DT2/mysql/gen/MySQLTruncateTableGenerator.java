package DT2.mysql.gen;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.mysql.MySQLGlobalState;

public final class MySQLTruncateTableGenerator {

    private MySQLTruncateTableGenerator() {
    }

    public static SQLQueryAdapter generate(MySQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        sb.append(globalState.getSchema().getRandomTable().getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("doesn't have this option"));
    }

}
