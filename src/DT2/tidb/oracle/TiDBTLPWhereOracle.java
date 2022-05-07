package DT2.tidb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.tidb.TiDBErrors;
import DT2.tidb.TiDBProvider.TiDBGlobalState;
import DT2.tidb.visitor.TiDBVisitor;

public class TiDBTLPWhereOracle extends TiDBTLPBase {

    public TiDBTLPWhereOracle(TiDBGlobalState state) {
        super(state);
        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = TiDBVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setWhereClause(predicate);
        String firstQueryString = TiDBVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = TiDBVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = TiDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
