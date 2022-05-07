package DT2.arangodb.test;

import static DT2.arangodb.ArangoDBComparatorHelper.assumeResultSetsAreEqual;
import static DT2.arangodb.ArangoDBComparatorHelper.getResultSetAsDocumentList;

import java.util.List;

import com.arangodb.entity.BaseDocument;

import DT2.arangodb.ArangoDBProvider;
import DT2.arangodb.query.ArangoDBSelectQuery;
import DT2.arangodb.visitor.ArangoDBVisitor;

public class ArangoDBQueryPartitioningWhereTester extends ArangoDBQueryPartitioningBase {
    public ArangoDBQueryPartitioningWhereTester(ArangoDBProvider.ArangoDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();
        select.setFilterClause(null);

        ArangoDBSelectQuery query = ArangoDBVisitor.asSelectQuery(select);
        List<BaseDocument> firstResultSet = getResultSetAsDocumentList(query, state);

        select.setFilterClause(predicate);
        query = ArangoDBVisitor.asSelectQuery(select);
        List<BaseDocument> secondResultSet = getResultSetAsDocumentList(query, state);

        select.setFilterClause(negatedPredicate);
        query = ArangoDBVisitor.asSelectQuery(select);
        List<BaseDocument> thirdResultSet = getResultSetAsDocumentList(query, state);

        thirdResultSet.addAll(secondResultSet);
        assumeResultSetsAreEqual(firstResultSet, thirdResultSet, query);

        if (state.getDbmsSpecificOptions().withOptimizerRuleTests) {
            select.setFilterClause(predicate);
            query = ArangoDBVisitor.asSelectQuery(select);
            query.excludeRandomOptRules();
            List<BaseDocument> forthResultSet = getResultSetAsDocumentList(query, state);
            assumeResultSetsAreEqual(secondResultSet, forthResultSet, query);
        }
    }
}
