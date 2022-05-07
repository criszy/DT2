package DT2.postgres.gen;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.PostgresSchema.PostgresTable;

public final class PostgresClusterGenerator {

    private PostgresClusterGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("there is no previously clustered index for table");
        errors.add("cannot cluster a partitioned table");
        errors.add("access method does not support clustering");
        StringBuilder sb = new StringBuilder("CLUSTER ");
        if (Randomly.getBoolean()) {
            PostgresTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
            sb.append(table.getName());
            if (Randomly.getBoolean() && !table.getIndexes().isEmpty()) {
                sb.append(" USING ");
                sb.append(table.getRandomIndex().getIndexName());
                errors.add("cannot cluster on partial index");
            }
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
