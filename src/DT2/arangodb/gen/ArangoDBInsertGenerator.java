package DT2.arangodb.gen;

import com.arangodb.entity.BaseDocument;

import DT2.arangodb.ArangoDBProvider;
import DT2.arangodb.ArangoDBQueryAdapter;
import DT2.arangodb.ArangoDBSchema;
import DT2.arangodb.query.ArangoDBConstantGenerator;
import DT2.arangodb.query.ArangoDBInsertQuery;

public final class ArangoDBInsertGenerator {

    private final ArangoDBProvider.ArangoDBGlobalState globalState;

    private ArangoDBInsertGenerator(ArangoDBProvider.ArangoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static ArangoDBQueryAdapter getQuery(ArangoDBProvider.ArangoDBGlobalState globalState) {
        return new ArangoDBInsertGenerator(globalState).generate();
    }

    private ArangoDBQueryAdapter generate() {
        BaseDocument result = new BaseDocument();
        ArangoDBSchema.ArangoDBTable table = globalState.getSchema().getRandomTable();
        ArangoDBConstantGenerator constantGenerator = new ArangoDBConstantGenerator(globalState);

        for (int i = 0; i < table.getColumns().size(); i++) {
            if (!globalState.getDbmsSpecificOptions().testRandomTypeInserts) {
                constantGenerator.addRandomConstantWithType(result, table.getColumns().get(i).getName(),
                        table.getColumns().get(i).getType());
            } else {
                constantGenerator.addRandomConstant(result, table.getColumns().get(i).getName());
            }
        }

        return new ArangoDBInsertQuery(table, result);
    }
}
