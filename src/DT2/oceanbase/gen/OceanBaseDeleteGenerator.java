package DT2.oceanbase.gen;

import java.util.Arrays;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.oceanbase.OceanBaseErrors;
import DT2.oceanbase.OceanBaseGlobalState;
import DT2.oceanbase.OceanBaseSchema.OceanBaseTable;
import DT2.oceanbase.OceanBaseVisitor;

public class OceanBaseDeleteGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final OceanBaseGlobalState globalState;
    private final Randomly r;

    public OceanBaseDeleteGenerator(OceanBaseGlobalState globalState) {
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public static SQLQueryAdapter delete(OceanBaseGlobalState globalState) {
        return new OceanBaseDeleteGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        OceanBaseTable randomTable = globalState.getSchema().getRandomTable();
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState)
                .setColumns(randomTable.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("DELETE");
        if (Randomly.getBoolean()) {
            sb.append(" /*+parallel(" + r.getLong(0, 10) + ") enable_parallel_dml*/ ");
        }
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(OceanBaseVisitor.asString(gen.generateExpression()));
            OceanBaseErrors.addExpressionErrors(errors);
        }
        errors.addAll(Arrays.asList("doesn't have this option", "Truncated incorrect DOUBLE value",
                "Truncated incorrect INTEGER value", "Truncated incorrect DECIMAL value",
                "Data truncated for functional index", "Incorrect value", "Out of range value for column",
                "Data truncation: %s value is out of range in '%s'"));
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
