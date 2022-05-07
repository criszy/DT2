package DT2.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import DT2.ComparatorHelper;
import DT2.IgnoreMeException;
import DT2.Randomly;
import DT2.common.ast.newast.NewAliasNode;
import DT2.common.ast.newast.NewBinaryOperatorNode;
import DT2.common.ast.newast.NewFunctionNode;
import DT2.common.ast.newast.NewUnaryPostfixOperatorNode;
import DT2.common.ast.newast.NewUnaryPrefixOperatorNode;
import DT2.common.ast.newast.Node;
import DT2.common.oracle.TestOracle;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.query.SQLancerResultSet;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import DT2.duckdb.DuckDBSchema.DuckDBDataType;
import DT2.duckdb.DuckDBToStringVisitor;
import DT2.duckdb.ast.DuckDBExpression;
import DT2.duckdb.ast.DuckDBSelect;
import DT2.duckdb.gen.DuckDBExpressionGenerator.DuckDBAggregateFunction;
import DT2.duckdb.gen.DuckDBExpressionGenerator.DuckDBBinaryArithmeticOperator;
import DT2.duckdb.gen.DuckDBExpressionGenerator.DuckDBCastOperation;
import DT2.duckdb.gen.DuckDBExpressionGenerator.DuckDBUnaryPostfixOperator;
import DT2.duckdb.gen.DuckDBExpressionGenerator.DuckDBUnaryPrefixOperator;

public class DuckDBQueryPartitioningAggregateTester extends DuckDBQueryPartitioningBase implements TestOracle {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public DuckDBQueryPartitioningAggregateTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        DuckDBAggregateFunction aggregateFunction = Randomly.fromOptions(DuckDBAggregateFunction.MAX,
                DuckDBAggregateFunction.MIN, DuckDBAggregateFunction.SUM, DuckDBAggregateFunction.COUNT,
                DuckDBAggregateFunction.AVG/* , DuckDBAggregateFunction.STDDEV_POP */);
        NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> aggregate = gen
                .generateArgsForAggregate(aggregateFunction);
        List<Node<DuckDBExpression>> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        originalQuery = DuckDBToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
                "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
        if (firstResult == null && secondResult != null
                || firstResult != null && (!firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult))) {
            if (secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(DuckDBSelect select,
            NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> aggregate, List<Node<DuckDBExpression>> from) {
        String metamorphicQuery;
        Node<DuckDBExpression> whereClause = gen.generateExpression();
        Node<DuckDBExpression> negatedClause = new NewUnaryPrefixOperatorNode<>(whereClause,
                DuckDBUnaryPrefixOperator.NOT);
        Node<DuckDBExpression> notNullClause = new NewUnaryPostfixOperatorNode<>(whereClause,
                DuckDBUnaryPostfixOperator.IS_NULL);
        List<Node<DuckDBExpression>> mappedAggregate = mapped(aggregate);
        DuckDBSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        DuckDBSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        DuckDBSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += DuckDBToStringVisitor.asString(leftSelect) + " UNION ALL "
                + DuckDBToStringVisitor.asString(middleSelect) + " UNION ALL "
                + DuckDBToStringVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getString(1);
            }
            return resultString;
        } catch (SQLException e) {
            if (!e.getMessage().contains("Not implemented type")) {
                throw new AssertionError(queryString, e);
            } else {
                throw new IgnoreMeException();
            }
        }
    }

    private List<Node<DuckDBExpression>> mapped(NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> aggregate) {
        DuckDBCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(Arrays.asList(aggregate));
        case AVG:
            NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> sum = new NewFunctionNode<>(aggregate.getArgs(),
                    DuckDBAggregateFunction.SUM);
            count = new DuckDBCastOperation(new NewFunctionNode<>(aggregate.getArgs(), DuckDBAggregateFunction.COUNT),
                    new DuckDBCompositeDataType(DuckDBDataType.FLOAT, 8));
            return aliasArgs(Arrays.asList(sum, count));
        case STDDEV_POP:
            NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> sumSquared = new NewFunctionNode<>(
                    Arrays.asList(new NewBinaryOperatorNode<>(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
                            DuckDBBinaryArithmeticOperator.MULT)),
                    DuckDBAggregateFunction.SUM);
            count = new DuckDBCastOperation(
                    new NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction>(aggregate.getArgs(),
                            DuckDBAggregateFunction.COUNT),
                    new DuckDBCompositeDataType(DuckDBDataType.FLOAT, 8));
            NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> avg = new NewFunctionNode<>(aggregate.getArgs(),
                    DuckDBAggregateFunction.AVG);
            return aliasArgs(Arrays.asList(sumSquared, count, avg));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<Node<DuckDBExpression>> aliasArgs(List<Node<DuckDBExpression>> originalAggregateArgs) {
        List<Node<DuckDBExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<DuckDBExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<DuckDBExpression>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
        case STDDEV_POP:
            return "sqrt(SUM(agg0)/SUM(agg1)-SUM(agg2)*SUM(agg2))";
        case AVG:
            return "SUM(agg0::FLOAT)/SUM(agg1)::FLOAT";
        case COUNT:
            return DuckDBAggregateFunction.SUM.toString() + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private DuckDBSelect getSelect(List<Node<DuckDBExpression>> aggregates, List<Node<DuckDBExpression>> from,
            Node<DuckDBExpression> whereClause, List<Node<DuckDBExpression>> joinList) {
        DuckDBSelect leftSelect = new DuckDBSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
