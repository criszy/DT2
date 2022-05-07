package DT2.mongodb.gen;

import java.util.List;

import DT2.Randomly;
import DT2.mongodb.MongoDBProvider.MongoDBGlobalState;
import DT2.mongodb.MongoDBQueryAdapter;
import DT2.mongodb.MongoDBSchema.MongoDBColumn;
import DT2.mongodb.MongoDBSchema.MongoDBTable;
import DT2.mongodb.query.MongoDBCreateIndexQuery;

public final class MongoDBIndexGenerator {
    private MongoDBIndexGenerator() {
    }

    public static MongoDBQueryAdapter getQuery(MongoDBGlobalState globalState) {
        MongoDBTable randomTable = globalState.getSchema().getRandomTable();
        List<MongoDBColumn> columns = Randomly.nonEmptySubset(randomTable.getColumns());
        MongoDBCreateIndexQuery createIndexQuery = new MongoDBCreateIndexQuery(randomTable);
        for (MongoDBColumn column : columns) {
            createIndexQuery.addIndex(column.getName(), Randomly.getBoolean());
        }
        return createIndexQuery;
    }
}
