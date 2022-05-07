package DT2.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.common.ast.newast.Node;
import DT2.common.oracle.TestOracle;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBToStringVisitor;
import DT2.duckdb.ast.DuckDBExpression;

public class DuckDBQueryPartitioningHavingTester extends DuckDBQueryPartitioningBase implements TestOracle {

    public DuckDBQueryPartitioningHavingTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
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
        String originalQueryString = DuckDBToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(predicate);
        String firstQueryString = DuckDBToStringVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = DuckDBToStringVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = DuckDBToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected Node<DuckDBExpression> generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<Node<DuckDBExpression>> generateFetchColumns() {
        return Arrays.asList(gen.generateHavingClause());
    }

}
