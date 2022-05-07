package DT2.oceanbase.gen;

import java.util.List;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.oceanbase.OceanBaseErrors;
import DT2.oceanbase.OceanBaseGlobalState;
import DT2.oceanbase.OceanBaseSchema;
import DT2.oceanbase.OceanBaseVisitor;

public class OceanBaseUpdateGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final OceanBaseGlobalState globalState;
    private final Randomly r;

    public OceanBaseUpdateGenerator(OceanBaseGlobalState globalState) {
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public static SQLQueryAdapter update(OceanBaseGlobalState globalState) {
        return new OceanBaseUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        ExpectedErrors errors = new ExpectedErrors();
        OceanBaseSchema.OceanBaseTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        if (Randomly.getBoolean()) {
            sb.append(" /*+parallel(" + r.getInteger(0, 10) + ") enable_parallel_dml*/ ");
        }
        sb.append(table.getName());
        sb.append(" SET ");
        List<OceanBaseSchema.OceanBaseColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            if (Randomly.getBoolean()) {
                sb.append(gen.generateConstant(columns.get(i)));
            } else {
                sb.append(OceanBaseVisitor.asString(gen.generateExpression()));
                OceanBaseErrors.addExpressionErrors(errors);
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            OceanBaseErrors.addExpressionErrors(errors);
            sb.append(OceanBaseVisitor.asString(gen.generateExpression()));
            errors.add("Data Too Long");
        }
        errors.add("Duplicated primary key");
        OceanBaseErrors.addInsertErrors(errors);

        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
