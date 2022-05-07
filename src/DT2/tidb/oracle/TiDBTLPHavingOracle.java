package DT2.tidb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.common.oracle.TestOracle;
import DT2.tidb.TiDBErrors;
import DT2.tidb.TiDBProvider.TiDBGlobalState;
import DT2.tidb.ast.TiDBExpression;
import DT2.tidb.visitor.TiDBVisitor;

public class TiDBTLPHavingOracle extends TiDBTLPBase implements TestOracle {

    public TiDBTLPHavingOracle(TiDBGlobalState state) {
        super(state);
        TiDBErrors.addExpressionHavingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = TiDBVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(predicate);
        String firstQueryString = TiDBVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = TiDBVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = TiDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected TiDBExpression generatePredicate() {
        return gen.generateHavingClause();
    }
}
