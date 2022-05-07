package DT2.postgres.gen;

import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;

public final class PostgresTruncateGenerator {

    private PostgresTruncateGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("TRUNCATE");
        if (Randomly.getBoolean()) {
            sb.append(" TABLE");
        }
        // TODO partitions
        // if (Randomly.getBoolean()) {
        // sb.append(" ONLY");
        // }
        sb.append(" ");
        sb.append(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty().stream().map(t -> t.getName())
                .collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("RESTART IDENTITY", "CONTINUE IDENTITY"));
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors
                .from("cannot truncate a table referenced in a foreign key constraint", "is not a table"));
    }

}
