package DT2.cosmos;

import com.google.auto.service.AutoService;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import DT2.DatabaseProvider;
import DT2.IgnoreMeException;
import DT2.ProviderAdapter;
import DT2.Randomly;
import DT2.StatementExecutor;
import DT2.common.log.LoggableFactory;
import DT2.mongodb.MongoDBConnection;
import DT2.mongodb.MongoDBLoggableFactory;
import DT2.mongodb.MongoDBOptions;
import DT2.mongodb.MongoDBQueryAdapter;
import DT2.mongodb.gen.MongoDBTableGenerator;

@AutoService(DatabaseProvider.class)
public class CosmosProvider extends
        ProviderAdapter<DT2.mongodb.MongoDBProvider.MongoDBGlobalState, MongoDBOptions, MongoDBConnection> {

    public CosmosProvider() {
        super(DT2.mongodb.MongoDBProvider.MongoDBGlobalState.class, MongoDBOptions.class);
    }

    @Override
    public void generateDatabase(DT2.mongodb.MongoDBProvider.MongoDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(4, 5, 6); i++) {
            boolean success;
            do {
                MongoDBQueryAdapter query = new MongoDBTableGenerator(globalState).getQuery(globalState);
                success = globalState.executeStatement(query);
            } while (!success);
        }
        StatementExecutor<DT2.mongodb.MongoDBProvider.MongoDBGlobalState, DT2.mongodb.MongoDBProvider.Action> se = new StatementExecutor<>(
                globalState, DT2.mongodb.MongoDBProvider.Action.values(),
                DT2.mongodb.MongoDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public MongoDBConnection createDatabase(DT2.mongodb.MongoDBProvider.MongoDBGlobalState globalState)
            throws Exception {
        String connectionString = "";
        if (connectionString.equals("")) {
            throw new AssertionError("Please set connection string for cosmos database, located in CosmosProvider");
        }
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString)).build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase(globalState.getDatabaseName());
        database.drop();
        return new MongoDBConnection(mongoClient, database);
    }

    @Override
    public String getDBMSName() {
        return "cosmos";
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new MongoDBLoggableFactory();
    }

    @Override
    protected void checkViewsAreValid(DT2.mongodb.MongoDBProvider.MongoDBGlobalState globalState) {
    }
}
