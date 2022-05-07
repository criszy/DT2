package DT2.mysql.gen.admin;

import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.query.SQLQueryAdapter;
import DT2.mysql.MySQLGlobalState;

public final class MySQLReset {

    private MySQLReset() {
    }

    public static SQLQueryAdapter create(MySQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("RESET ");
        sb.append(Randomly.nonEmptySubset("MASTER", "SLAVE").stream().collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
