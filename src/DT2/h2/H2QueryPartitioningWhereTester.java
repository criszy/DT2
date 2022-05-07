package DT2.h2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.h2.H2Provider.H2GlobalState;

public class H2QueryPartitioningWhereTester extends H2QueryPartitioningBase {

    public H2QueryPartitioningWhereTester(H2GlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = H2ToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setWhereClause(predicate);
        String firstQueryString = H2ToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = H2ToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = H2ToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
