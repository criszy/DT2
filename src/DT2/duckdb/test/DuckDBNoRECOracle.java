package DT2.duckdb.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import DT2.IgnoreMeException;
import DT2.Randomly;
import DT2.SQLConnection;
import DT2.common.ast.newast.ColumnReferenceNode;
import DT2.common.ast.newast.NewPostfixTextNode;
import DT2.common.ast.newast.Node;
import DT2.common.ast.newast.TableReferenceNode;
import DT2.common.oracle.NoRECBase;
import DT2.common.oracle.TestOracle;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.query.SQLancerResultSet;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema;
import DT2.duckdb.DuckDBSchema.DuckDBColumn;
import DT2.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import DT2.duckdb.DuckDBSchema.DuckDBDataType;
import DT2.duckdb.DuckDBSchema.DuckDBTable;
import DT2.duckdb.DuckDBSchema.DuckDBTables;
import DT2.duckdb.DuckDBToStringVisitor;
import DT2.duckdb.ast.DuckDBExpression;
import DT2.duckdb.ast.DuckDBJoin;
import DT2.duckdb.ast.DuckDBSelect;
import DT2.duckdb.gen.DuckDBExpressionGenerator;
import DT2.duckdb.gen.DuckDBExpressionGenerator.DuckDBCastOperation;

public class DuckDBNoRECOracle extends NoRECBase<DuckDBGlobalState> implements TestOracle {

    private final DuckDBSchema s;

    public DuckDBNoRECOracle(DuckDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        DuckDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        DuckDBTables randomTables = s.getRandomTableNonEmptyTables();
        List<DuckDBColumn> columns = randomTables.getColumns();
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(state).setColumns(columns);
        Node<DuckDBExpression> randomWhereCondition = gen.generateExpression();
        List<DuckDBTable> tables = randomTables.getTables();
        List<TableReferenceNode<DuckDBExpression, DuckDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DuckDBExpression, DuckDBTable>(t)).collect(Collectors.toList());
        List<Node<DuckDBExpression>> joins = DuckDBJoin.getJoins(tableList, state);
        int secondCount = getSecondQuery(tableList.stream().collect(Collectors.toList()), randomWhereCondition, joins);
        int firstCount = getFirstQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(List<Node<DuckDBExpression>> tableList, Node<DuckDBExpression> randomWhereCondition,
            List<Node<DuckDBExpression>> joins) throws SQLException {
        DuckDBSelect select = new DuckDBSelect();
        // select.setGroupByClause(groupBys);
        // DuckDBExpression isTrue = DuckDBPostfixOperation.create(randomWhereCondition,
        // PostfixOperator.IS_TRUE);
        Node<DuckDBExpression> asText = new NewPostfixTextNode<>(new DuckDBCastOperation(
                new NewPostfixTextNode<DuckDBExpression>(randomWhereCondition,
                        " IS NOT NULL AND " + DuckDBToStringVisitor.asString(randomWhereCondition)),
                new DuckDBCompositeDataType(DuckDBDataType.INT, 8)), "as count");
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(tableList);
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + DuckDBToStringVisitor.asString(select) + ") as res";
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getFirstQueryCount(SQLConnection con, List<Node<DuckDBExpression>> tableList,
            List<DuckDBColumn> columns, Node<DuckDBExpression> randomWhereCondition, List<Node<DuckDBExpression>> joins)
            throws SQLException {
        DuckDBSelect select = new DuckDBSelect();
        // select.setGroupByClause(groupBys);
        // DuckDBAggregate aggr = new DuckDBAggregate(
        List<Node<DuckDBExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(c)).collect(Collectors.toList());
        // DuckDBAggregateFunction.COUNT);
        // select.setFetchColumns(Arrays.asList(aggr));
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new DuckDBExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = DuckDBToStringVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
