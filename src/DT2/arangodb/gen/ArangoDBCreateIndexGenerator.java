package DT2.arangodb.gen;

import DT2.arangodb.ArangoDBProvider;
import DT2.arangodb.ArangoDBQueryAdapter;
import DT2.arangodb.ArangoDBSchema;
import DT2.arangodb.query.ArangoDBCreateIndexQuery;

public final class ArangoDBCreateIndexGenerator {
    private ArangoDBCreateIndexGenerator() {

    }

    public static ArangoDBQueryAdapter getQuery(ArangoDBProvider.ArangoDBGlobalState globalState) {
        ArangoDBSchema.ArangoDBTable randomTable = globalState.getSchema().getRandomTable();
        ArangoDBSchema.ArangoDBColumn column = randomTable.getRandomColumn();
        return new ArangoDBCreateIndexQuery(column);
    }
}
