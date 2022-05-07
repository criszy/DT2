package DT2.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.cockroachdb.CockroachDBErrors;
import DT2.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import DT2.cockroachdb.CockroachDBVisitor;
import DT2.cockroachdb.ast.CockroachDBBinaryLogicalOperation;
import DT2.cockroachdb.ast.CockroachDBBinaryLogicalOperation.CockroachDBBinaryLogicalOperator;
import DT2.cockroachdb.ast.CockroachDBExpression;
import DT2.cockroachdb.ast.CockroachDBNotOperation;
import DT2.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import DT2.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;

public class CockroachDBTLPExtendedWhereOracle extends CockroachDBTLPBase {

    private CockroachDBExpression originalPredicate;

    public CockroachDBTLPExtendedWhereOracle(CockroachDBGlobalState state) {
        super(state);
        CockroachDBErrors.addExpressionErrors(errors);
        errors.add("GROUP BY term out of range");
    }

    @Override
    public void check() throws SQLException {
        super.check();
        originalPredicate = generatePredicate();
        select.setWhereClause(originalPredicate);
        String originalQueryString = CockroachDBVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean allowOrderBy = Randomly.getBoolean();
        if (allowOrderBy) {
            select.setOrderByExpressions(gen.getOrderingTerms());
        }
        select.setWhereClause(combinePredicate(predicate));
        String firstQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(combinePredicate(new CockroachDBNotOperation(predicate)));
        String secondQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(combinePredicate(
                new CockroachDBUnaryPostfixOperation(predicate, CockroachDBUnaryPostfixOperator.IS_NULL)));
        String thirdQueryString = CockroachDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !allowOrderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    public CockroachDBExpression combinePredicate(CockroachDBExpression expr) {
        return new CockroachDBBinaryLogicalOperation(originalPredicate, expr, CockroachDBBinaryLogicalOperator.AND);

    }
}
