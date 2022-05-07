package DT2.postgres.gen;

import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.PostgresSchema.PostgresTable;

public final class PostgresAnalyzeGenerator {

    private PostgresAnalyzeGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder("ANALYZE");
        if (Randomly.getBoolean()) {
            sb.append("(");
            if (Randomly.getBoolean()) {
                sb.append(" VERBOSE");
            } else {
                sb.append(" SKIP_LOCKED");
            }
            sb.append(")");
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(table.getName());
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
                        .collect(Collectors.joining(", ")));
                sb.append(")");
            }
        }
        // FIXME: bug in postgres?
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("deadlock"));
    }

}
