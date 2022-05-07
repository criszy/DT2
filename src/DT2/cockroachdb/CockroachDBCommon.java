package DT2.cockroachdb;

import java.util.ArrayList;
import java.util.List;

import DT2.Randomly;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBTable;
import DT2.cockroachdb.ast.CockroachDBExpression;
import DT2.cockroachdb.ast.CockroachDBIndexReference;
import DT2.cockroachdb.ast.CockroachDBTableReference;

public final class CockroachDBCommon {

    private CockroachDBCommon() {
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("en", "de", "es", "cmn");
    }

    public static List<CockroachDBExpression> getTableReferences(List<CockroachDBTableReference> tableList) {
        List<CockroachDBExpression> from = new ArrayList<>();
        for (CockroachDBTableReference t : tableList) {
            CockroachDBTable table = t.getTable();
            if (!table.getIndexes().isEmpty() && Randomly.getBooleanWithSmallProbability()) {
                from.add(new CockroachDBIndexReference(t, table.getRandomIndex()));
            } else {
                from.add(t);
            }
        }
        return from;
    }

}
