package DT2.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBToStringVisitor;

public class DuckDBQueryPartitioningDistinctTester extends DuckDBQueryPartitioningBase {

    public DuckDBQueryPartitioningDistinctTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setDistinct(true);
        select.setWhereClause(null);
        String originalQueryString = DuckDBToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        if (Randomly.getBoolean()) {
            select.setDistinct(false);
        }
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

}
