package DT2.oceanbase.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.oceanbase.OceanBaseGlobalState;
import DT2.oceanbase.OceanBaseVisitor;

public class OceanBaseTLPWhereOracle extends OceanBaseTLPBase {

    public OceanBaseTLPWhereOracle(OceanBaseGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = OceanBaseVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setOrderByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = OceanBaseVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = OceanBaseVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = OceanBaseVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
