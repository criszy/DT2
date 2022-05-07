package DT2.mongodb.gen;

import java.util.ArrayList;
import java.util.List;

import DT2.Randomly;
import DT2.mongodb.MongoDBProvider.MongoDBGlobalState;
import DT2.mongodb.MongoDBQueryAdapter;
import DT2.mongodb.MongoDBSchema.MongoDBColumn;
import DT2.mongodb.MongoDBSchema.MongoDBDataType;
import DT2.mongodb.MongoDBSchema.MongoDBTable;
import DT2.mongodb.query.MongoDBCreateTableQuery;

public class MongoDBTableGenerator {

    private MongoDBTable table;
    private final List<MongoDBColumn> columnsToBeAdded = new ArrayList<>();
    private final MongoDBGlobalState state;

    public MongoDBTableGenerator(MongoDBGlobalState state) {
        this.state = state;
    }

    public MongoDBQueryAdapter getQuery(MongoDBGlobalState globalState) {
        String tableName = globalState.getSchema().getFreeTableName();
        MongoDBCreateTableQuery createTableQuery = new MongoDBCreateTableQuery(tableName);
        table = new MongoDBTable(tableName, columnsToBeAdded, false);
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            MongoDBDataType type = createColumn(columnName);
            if (globalState.getDbmsSpecificOptions().testValidation) {
                createTableQuery.addValidation(columnName, type.getBsonType());
            }
        }
        globalState.addTable(table);
        return createTableQuery;
    }

    private MongoDBDataType createColumn(String columnName) {
        MongoDBDataType columnType = MongoDBDataType.getRandom(state);
        MongoDBColumn newColumn = new MongoDBColumn(columnName, columnType, false, false);
        newColumn.setTable(table);
        columnsToBeAdded.add(newColumn);
        return columnType;
    }

    public String getTableName() {
        return table.getName();
    }

    public MongoDBTable getGeneratedTable() {
        return table;
    }
}
