package DT2.oceanbase.gen;

import DT2.IgnoreMeException;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.oceanbase.OceanBaseGlobalState;
import DT2.oceanbase.OceanBaseSchema.OceanBaseTable;

public final class OceanBaseDropIndex {

    private OceanBaseDropIndex() {
    }

    public static SQLQueryAdapter generate(OceanBaseGlobalState globalState) {
        OceanBaseTable table = globalState.getSchema().getRandomTable();
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName());
        sb.append(" ON ");
        sb.append(table.getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("LOCK=NONE is not supported",
                "ALGORITHM=INPLACE is not supported", "Data truncation", "Data truncated for functional index"));
    }

}
