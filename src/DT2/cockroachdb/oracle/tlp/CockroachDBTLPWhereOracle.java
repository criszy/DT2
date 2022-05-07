package DT2.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import DT2.cockroachdb.CockroachDBVisitor;
import DT2.cockroachdb.ast.CockroachDBExpression;
import DT2.cockroachdb.ast.CockroachDBNotOperation;
import DT2.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import DT2.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;

public class CockroachDBTLPWhereOracle extends CockroachDBTLPBase {

    public CockroachDBTLPWhereOracle(CockroachDBGlobalState state) {
        super(state);
        errors.add("GROUP BY term out of range");
    }

    @Override
    public void check() throws SQLException {
        super.check();
        String originalQueryString = CockroachDBVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean allowOrderBy = Randomly.getBoolean();
        if (allowOrderBy) {
            select.setOrderByExpressions(gen.getOrderingTerms());
        }
        CockroachDBExpression predicate = gen.generateExpression(CockroachDBDataType.BOOL.get());
        select.setWhereClause(predicate);
        String firstQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(new CockroachDBNotOperation(predicate));
        String secondQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(new CockroachDBUnaryPostfixOperation(predicate, CockroachDBUnaryPostfixOperator.IS_NULL));
        String thirdQueryString = CockroachDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !allowOrderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
