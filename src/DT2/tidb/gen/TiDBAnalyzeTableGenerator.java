package DT2.tidb.gen;

import java.sql.SQLException;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.tidb.TiDBProvider.TiDBGlobalState;
import DT2.tidb.TiDBSchema.TiDBTable;

public final class TiDBAnalyzeTableGenerator {

    private TiDBAnalyzeTableGenerator() {
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        boolean analyzeIndex = !table.getIndexes().isEmpty() && Randomly.getBoolean();
        StringBuilder sb = new StringBuilder("ANALYZE ");
        if (analyzeIndex && Randomly.getBoolean()) {
            sb.append("INCREMENTAL ");
        }
        sb.append("TABLE ");
        sb.append(table.getName());
        if (analyzeIndex) {
            sb.append(" INDEX ");
            sb.append(table.getRandomIndex().getIndexName());
        }
        if (Randomly.getBoolean()) {
            sb.append(" WITH ");
            sb.append(Randomly.getNotCachedInteger(1, 1024));
            sb.append(" BUCKETS");
        }
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("https://github.com/pingcap/tidb/issues/15993",
                        /* https://github.com/pingcap/tidb/issues/15993 */ "doesn't have a default value",
                        "Index 'PRIMARY' in field list does not exist in table" /*
                                                                                 * https://github. com/pingcap/tidb/
                                                                                 * issues/15993
                                                                                 */));
    }

}
