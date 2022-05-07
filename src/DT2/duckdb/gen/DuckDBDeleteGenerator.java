package DT2.duckdb.gen;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema.DuckDBTable;
import DT2.duckdb.DuckDBToStringVisitor;

public final class DuckDBDeleteGenerator {

    private DuckDBDeleteGenerator() {
    }

    public static SQLQueryAdapter generate(DuckDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(DuckDBToStringVisitor.asString(
                    new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        DuckDBErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
