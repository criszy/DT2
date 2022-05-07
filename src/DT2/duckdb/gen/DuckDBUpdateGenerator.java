package DT2.duckdb.gen;

import java.util.List;

import DT2.Randomly;
import DT2.common.ast.newast.Node;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema.DuckDBColumn;
import DT2.duckdb.DuckDBSchema.DuckDBTable;
import DT2.duckdb.DuckDBToStringVisitor;
import DT2.duckdb.ast.DuckDBExpression;

public final class DuckDBUpdateGenerator {

    private DuckDBUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        ExpectedErrors errors = new ExpectedErrors();
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(" SET ");
        List<DuckDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            Node<DuckDBExpression> expr;
            if (Randomly.getBooleanWithSmallProbability()) {
                expr = gen.generateExpression();
                DuckDBErrors.addExpressionErrors(errors);
            } else {
                expr = gen.generateConstant();
            }
            sb.append(DuckDBToStringVisitor.asString(expr));
        }
        DuckDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
