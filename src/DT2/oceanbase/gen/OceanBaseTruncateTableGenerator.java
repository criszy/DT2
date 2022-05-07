package DT2.oceanbase.gen;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.oceanbase.OceanBaseGlobalState;

public final class OceanBaseTruncateTableGenerator {

    private OceanBaseTruncateTableGenerator() {
    }

    public static SQLQueryAdapter generate(OceanBaseGlobalState globalState) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        sb.append(globalState.getSchema().getRandomTable().getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("doesn't have this option"));
    }

}
