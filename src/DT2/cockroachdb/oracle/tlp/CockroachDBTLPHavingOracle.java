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

public class CockroachDBTLPHavingOracle extends CockroachDBTLPBase {

    public CockroachDBTLPHavingOracle(CockroachDBGlobalState state) {
        super(state);
        errors.add("GROUP BY term out of range");
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
        }
        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = CockroachDBVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(predicate);
        String firstQueryString = CockroachDBVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = CockroachDBVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = CockroachDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected CockroachDBExpression generatePredicate() {
        return gen.generateHavingClause();
    }

}
