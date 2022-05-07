package DT2.sqlite3.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.sqlite3.SQLite3GlobalState;
import DT2.sqlite3.SQLite3Visitor;
import DT2.sqlite3.ast.SQLite3Select.SelectType;

public class SQLite3TLPDistinctOracle extends SQLite3TLPBase {

    public SQLite3TLPDistinctOracle(SQLite3GlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setSelectType(SelectType.DISTINCT);
        select.setWhereClause(null);
        String originalQueryString = SQLite3Visitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = SQLite3Visitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = SQLite3Visitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = SQLite3Visitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
