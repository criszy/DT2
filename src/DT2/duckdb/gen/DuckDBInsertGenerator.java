package DT2.duckdb.gen;

import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.gen.AbstractInsertGenerator;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema.DuckDBColumn;
import DT2.duckdb.DuckDBSchema.DuckDBTable;
import DT2.duckdb.DuckDBToStringVisitor;

public class DuckDBInsertGenerator extends AbstractInsertGenerator<DuckDBColumn> {

    private final DuckDBGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public DuckDBInsertGenerator(DuckDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        return new DuckDBInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT INTO ");
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DuckDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns);
        DuckDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(DuckDBColumn tiDBColumn) {
        // TODO: select a more meaningful value
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("DEFAULT");
        } else {
            sb.append(DuckDBToStringVisitor.asString(new DuckDBExpressionGenerator(globalState).generateConstant()));
        }
    }

}
