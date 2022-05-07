package DT2.cockroachdb.gen;

import java.util.List;

import DT2.IgnoreMeException;
import DT2.Randomly;
import DT2.cockroachdb.CockroachDBErrors;
import DT2.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBTable;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.schema.TableIndex;

public final class CockroachDBCommentOnGenerator {

    private CockroachDBCommentOnGenerator() {
    }

    private enum Option {
        TABLE, INDEX, COLUMN
    }

    public static SQLQueryAdapter comment(CockroachDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("COMMENT ON ");
        CockroachDBTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        switch (Randomly.fromOptions(Option.values())) {
        case TABLE:
            sb.append("TABLE ");
            sb.append(randomTable.getName());
            break;
        case INDEX:
            List<TableIndex> indexes = randomTable.getIndexes();
            if (indexes.isEmpty()) {
                throw new IgnoreMeException();
            }
            TableIndex index = Randomly.fromList(indexes);
            if (index.getIndexName().contains("primary")) {
                throw new IgnoreMeException();
            }
            sb.append("INDEX ");
            sb.append(index.getIndexName());
            break;
        case COLUMN:
            sb.append("COLUMN ");
            CockroachDBColumn randomColumn = randomTable.getRandomColumn();

            sb.append(randomColumn.getFullQualifiedName());
            break;
        default:
            throw new AssertionError();
        }
        sb.append(" IS '");
        sb.append(globalState.getRandomly().getString().replace("'", "''"));
        sb.append("'");
        ExpectedErrors errors = new ExpectedErrors();
        CockroachDBErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
