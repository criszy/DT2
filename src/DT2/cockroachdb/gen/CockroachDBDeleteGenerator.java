package DT2.cockroachdb.gen;

import DT2.Randomly;
import DT2.cockroachdb.CockroachDBErrors;
import DT2.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBTable;
import DT2.cockroachdb.CockroachDBVisitor;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;

public final class CockroachDBDeleteGenerator {

    private CockroachDBDeleteGenerator() {
    }

    public static SQLQueryAdapter delete(CockroachDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append("DELETE FROM ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            CockroachDBErrors.addExpressionErrors(errors);
            sb.append(CockroachDBVisitor.asString(new CockroachDBExpressionGenerator(globalState)
                    .setColumns(table.getColumns()).generateExpression(CockroachDBDataType.BOOL.get())));
        } else {
            errors.add("rejected: DELETE without WHERE clause (sql_safe_updates = true)");
        }
        errors.add("foreign key violation");
        CockroachDBErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
