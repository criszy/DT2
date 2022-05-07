package DT2.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.common.ast.newast.ColumnReferenceNode;
import DT2.common.ast.newast.Node;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema.DuckDBColumn;
import DT2.duckdb.DuckDBToStringVisitor;
import DT2.duckdb.ast.DuckDBExpression;

public class DuckDBQueryPartitioningGroupByTester extends DuckDBQueryPartitioningBase {

    public DuckDBQueryPartitioningGroupByTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setGroupByExpressions(select.getFetchColumns());
        select.setWhereClause(null);
        String originalQueryString = DuckDBToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = DuckDBToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = DuckDBToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = DuckDBToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    List<Node<DuckDBExpression>> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(c)).collect(Collectors.toList());
    }

}
