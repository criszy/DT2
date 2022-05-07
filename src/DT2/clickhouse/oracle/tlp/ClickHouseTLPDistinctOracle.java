package DT2.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.clickhouse.ClickHouseProvider;
import DT2.clickhouse.ClickHouseVisitor;
import DT2.clickhouse.ast.ClickHouseSelect;

public class ClickHouseTLPDistinctOracle extends ClickHouseTLPBase {

    public ClickHouseTLPDistinctOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setSelectType(ClickHouseSelect.SelectType.DISTINCT);
        select.setWhereClause(null);
        String originalQueryString = ClickHouseVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = ClickHouseVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = ClickHouseVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = ClickHouseVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, false, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
