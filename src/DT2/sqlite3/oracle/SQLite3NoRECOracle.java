package DT2.sqlite3.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import DT2.IgnoreMeException;
import DT2.Randomly;
import DT2.common.oracle.NoRECBase;
import DT2.common.oracle.TestOracle;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.query.SQLancerResultSet;
import DT2.sqlite3.SQLite3Errors;
import DT2.sqlite3.SQLite3GlobalState;
import DT2.sqlite3.SQLite3Visitor;
import DT2.sqlite3.ast.SQLite3Aggregate;
import DT2.sqlite3.ast.SQLite3Expression;
import DT2.sqlite3.ast.SQLite3Expression.Join;
import DT2.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import DT2.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import DT2.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import DT2.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import DT2.sqlite3.ast.SQLite3Select;
import DT2.sqlite3.gen.SQLite3Common;
import DT2.sqlite3.gen.SQLite3ExpressionGenerator;
import DT2.sqlite3.schema.SQLite3Schema;
import DT2.sqlite3.schema.SQLite3Schema.SQLite3Column;
import DT2.sqlite3.schema.SQLite3Schema.SQLite3Table;
import DT2.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3NoRECOracle extends NoRECBase<SQLite3GlobalState> implements TestOracle {

    private static final int NO_VALID_RESULT = -1;
    private final SQLite3Schema s;
    private SQLite3ExpressionGenerator gen;

    public SQLite3NoRECOracle(SQLite3GlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addMatchQueryErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        errors.add("misuse of aggregate");
        errors.add("misuse of window function");
        errors.add("second argument to nth_value must be a positive integer");
        errors.add("no such table");
        errors.add("no query solution");
        errors.add("unable to use function MATCH in the requested context");
    }

    @Override
    public void check() throws SQLException {
        SQLite3Tables randomTables = s.getRandomTableNonEmptyTables();
        List<SQLite3Column> columns = randomTables.getColumns();
        gen = new SQLite3ExpressionGenerator(state).setColumns(columns);
        SQLite3Expression randomWhereCondition = gen.generateExpression();
        List<SQLite3Table> tables = randomTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
        SQLite3Select select = new SQLite3Select();
        select.setFromTables(tableRefs);
        select.setJoinClauses(joinStatements);

        int optimizedCount = getOptimizedQuery(select, randomWhereCondition);
        int unoptimizedCount = getUnoptimizedQuery(select, randomWhereCondition);
        if (optimizedCount == NO_VALID_RESULT || unoptimizedCount == NO_VALID_RESULT) {
            throw new IgnoreMeException();
        }
        if (optimizedCount != unoptimizedCount) {
            state.getState().getLocalState().log(optimizedQueryString + ";\n" + unoptimizedQueryString + ";");
            throw new AssertionError(optimizedCount + " " + unoptimizedCount);
        }

    }

    private int getUnoptimizedQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        SQLite3PostfixUnaryOperation isTrue = new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.IS_TRUE,
                randomWhereCondition);
        SQLite3PostfixText asText = new SQLite3PostfixText(isTrue, " as count", null);
        select.setFetchColumns(Arrays.asList(asText));
        select.setWhereClause(null);
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + SQLite3Visitor.asString(select) + ")";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        return extractCounts(q);
    }

    private int getOptimizedQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        boolean useAggregate = Randomly.getBoolean();
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (useAggregate) {
            select.setFetchColumns(Arrays.asList(new SQLite3Aggregate(Collections.emptyList(),
                    SQLite3Aggregate.SQLite3AggregateFunction.COUNT_ALL)));
        } else {
            SQLite3ColumnName aggr = new SQLite3ColumnName(SQLite3Column.createDummy("*"), null);
            select.setFetchColumns(Arrays.asList(aggr));
        }
        select.setWhereClause(randomWhereCondition);
        optimizedQueryString = SQLite3Visitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(optimizedQueryString);
        }
        SQLQueryAdapter q = new SQLQueryAdapter(optimizedQueryString, errors);
        return useAggregate ? extractCounts(q) : countRows(q);
    }

    private int countRows(SQLQueryAdapter q) {
        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null) {
                return NO_VALID_RESULT;
            } else {
                try {
                    while (rs.next()) {
                        count++;
                    }
                } catch (SQLException e) {
                    count = NO_VALID_RESULT;
                }
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(unoptimizedQueryString, e);
        }
        return count;
    }

    private int extractCounts(SQLQueryAdapter q) {
        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null) {
                return NO_VALID_RESULT;
            } else {
                try {
                    while (rs.next()) {
                        count += rs.getInt(1);
                    }
                } catch (SQLException e) {
                    count = NO_VALID_RESULT;
                }
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(unoptimizedQueryString, e);
        }
        return count;
    }

}
