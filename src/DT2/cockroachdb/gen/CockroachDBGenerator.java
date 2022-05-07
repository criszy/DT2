package DT2.cockroachdb.gen;

import java.util.List;

import DT2.Randomly;
import DT2.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import DT2.common.gen.AbstractGenerator;

public abstract class CockroachDBGenerator extends AbstractGenerator {

    protected final CockroachDBGlobalState globalState;

    public CockroachDBGenerator(CockroachDBGlobalState globalState) {
        this.globalState = globalState;
    }

    static void addColumns(StringBuilder sb, List<CockroachDBColumn> columns, boolean allowOrdering) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            if (allowOrdering && Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
    }

}
