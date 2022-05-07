package DT2.duckdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.ast.newast.Node;
import DT2.common.gen.UntypedExpressionGenerator;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema.DuckDBColumn;
import DT2.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import DT2.duckdb.DuckDBSchema.DuckDBDataType;
import DT2.duckdb.DuckDBToStringVisitor;
import DT2.duckdb.ast.DuckDBExpression;

public class DuckDBTableGenerator {

    public SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        List<DuckDBColumn> columns = getNewColumns();
        UntypedExpressionGenerator<Node<DuckDBExpression>, DuckDBColumn> gen = new DuckDBExpressionGenerator(
                globalState).setColumns(columns);
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());
            if (globalState.getDbmsSpecificOptions().testCollate && Randomly.getBooleanWithRatherLowProbability()
                    && columns.get(i).getType().getPrimitiveDataType() == DuckDBDataType.VARCHAR) {
                sb.append(" COLLATE ");
                sb.append(getRandomCollate());
            }
            if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" UNIQUE");
            }
            if (globalState.getDbmsSpecificOptions().testNotNullConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
            }
            if (globalState.getDbmsSpecificOptions().testCheckConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" CHECK(");
                sb.append(DuckDBToStringVisitor.asString(gen.generateExpression()));
                DuckDBErrors.addExpressionErrors(errors);
                sb.append(")");
            }
            if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testDefaultValues) {
                sb.append(" DEFAULT(");
                sb.append(DuckDBToStringVisitor.asString(gen.generateConstant()));
                DuckDBErrors.addExpressionErrors(errors);
                sb.append(")");
            }
        }
        if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBoolean()) {
            errors.add("Invalid type for index");
            List<DuckDBColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
            sb.append(", PRIMARY KEY(");
            sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX");
    }

    private static List<DuckDBColumn> getNewColumns() {
        List<DuckDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            DuckDBCompositeDataType columnType = DuckDBCompositeDataType.getRandom();
            columns.add(new DuckDBColumn(columnName, columnType, false, false));
        }
        return columns;
    }

}
