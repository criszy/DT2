package DT2.mongodb.test;

import static DT2.mongodb.MongoDBComparatorHelper.getResultSetAsDocumentList;

import java.util.List;

import org.bson.Document;

import DT2.Randomly;
import DT2.mongodb.MongoDBProvider;
import DT2.mongodb.MongoDBQueryAdapter;
import DT2.mongodb.gen.MongoDBInsertGenerator;
import DT2.mongodb.query.MongoDBRemoveQuery;
import DT2.mongodb.query.MongoDBSelectQuery;

public class MongoDBDocumentRemovalTester extends MongoDBDocumentRemovalBase {
    public MongoDBDocumentRemovalTester(MongoDBProvider.MongoDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();

        select.setWithCountClause(false);

        select.setFilterClause(predicate);
        MongoDBSelectQuery selectQuery = new MongoDBSelectQuery(select);
        List<Document> firstResultSet = getResultSetAsDocumentList(selectQuery, state);
        if (firstResultSet == null || firstResultSet.isEmpty()) {
            return;
        }

        Document documentToRemove = Randomly.fromList(firstResultSet);
        MongoDBRemoveQuery removeQuery = new MongoDBRemoveQuery(mainTable, documentToRemove.get("_id").toString());
        state.executeStatement(removeQuery);

        selectQuery = new MongoDBSelectQuery(select);
        List<Document> secondResultSet = getResultSetAsDocumentList(selectQuery, state);

        MongoDBQueryAdapter insertQuery = MongoDBInsertGenerator.getQuery(state);
        state.executeStatement(insertQuery);

        if (secondResultSet.size() + 1 != firstResultSet.size()) {
            String assertMessage = "The Result Sizes mismatches!";
            throw new AssertionError(assertMessage);
        }
    }
}
