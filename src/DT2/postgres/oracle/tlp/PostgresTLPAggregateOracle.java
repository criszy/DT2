package DT2.postgres.oracle.tlp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.postgresql.util.PSQLException;

import DT2.ComparatorHelper;
import DT2.IgnoreMeException;
import DT2.Randomly;
import DT2.common.oracle.TestOracle;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.query.SQLancerResultSet;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.PostgresSchema.PostgresDataType;
import DT2.postgres.PostgresVisitor;
import DT2.postgres.ast.PostgresAggregate;
import DT2.postgres.ast.PostgresAggregate.PostgresAggregateFunction;
import DT2.postgres.ast.PostgresAlias;
import DT2.postgres.ast.PostgresExpression;
import DT2.postgres.ast.PostgresJoin;
import DT2.postgres.ast.PostgresPostfixOperation;
import DT2.postgres.ast.PostgresPostfixOperation.PostfixOperator;
import DT2.postgres.ast.PostgresPrefixOperation;
import DT2.postgres.ast.PostgresPrefixOperation.PrefixOperator;
import DT2.postgres.ast.PostgresSelect;
import DT2.postgres.gen.PostgresCommon;

public class PostgresTLPAggregateOracle extends PostgresTLPBase implements TestOracle {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public PostgresTLPAggregateOracle(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addGroupingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        aggregateCheck();
    }

    protected void aggregateCheck() throws SQLException {
        PostgresAggregateFunction aggregateFunction = Randomly.fromOptions(PostgresAggregateFunction.MAX,
                PostgresAggregateFunction.MIN, PostgresAggregateFunction.SUM, PostgresAggregateFunction.BIT_AND,
                PostgresAggregateFunction.BIT_OR, PostgresAggregateFunction.BOOL_AND, PostgresAggregateFunction.BOOL_OR,
                PostgresAggregateFunction.COUNT);
        PostgresAggregate aggregate = gen.generateArgsForAggregate(aggregateFunction.getRandomReturnType(),
                aggregateFunction);
        List<PostgresExpression> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        originalQuery = PostgresVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        String queryFormatString = "-- %s;\n-- result: %s";
        String firstQueryString = String.format(queryFormatString, originalQuery, firstResult);
        String secondQueryString = String.format(queryFormatString, metamorphicQuery, secondResult);
        state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
        if (firstResult == null && secondResult != null || firstResult != null && secondResult == null
                || firstResult != null && !firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult)) {
            if (secondResult != null && secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            String assertionMessage = String.format("the results mismatch!\n%s\n%s", firstQueryString,
                    secondQueryString);
            throw new AssertionError(assertionMessage);
        }
    }

    private String createMetamorphicUnionQuery(PostgresSelect select, PostgresAggregate aggregate,
            List<PostgresExpression> from) {
        String metamorphicQuery;
        PostgresExpression whereClause = gen.generateExpression(PostgresDataType.BOOLEAN);
        PostgresExpression negatedClause = new PostgresPrefixOperation(whereClause, PrefixOperator.NOT);
        PostgresExpression notNullClause = new PostgresPostfixOperation(whereClause, PostfixOperator.IS_NULL);
        List<PostgresExpression> mappedAggregate = mapped(aggregate);
        PostgresSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinClauses());
        PostgresSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinClauses());
        PostgresSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinClauses());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += PostgresVisitor.asString(leftSelect) + " UNION ALL "
                + PostgresVisitor.asString(middleSelect) + " UNION ALL " + PostgresVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        // log TLP Aggregate SELECT queries on the current log file
        if (state.getOptions().logEachSelect()) {
            // TODO: refactor me
            state.getLogger().writeCurrent(queryString);
            try {
                state.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
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
        } catch (PSQLException e) {
            throw new AssertionError(queryString, e);
        }
        return resultString;
    }

    private List<PostgresExpression> mapped(PostgresAggregate aggregate) {
        switch (aggregate.getFunction()) {
        case SUM:
        case COUNT:
        case BIT_AND:
        case BIT_OR:
        case BOOL_AND:
        case BOOL_OR:
        case MAX:
        case MIN:
            return aliasArgs(Arrays.asList(aggregate));
        // case AVG:
        //// List<PostgresExpression> arg = Arrays.asList(new
        // PostgresCast(aggregate.getExpr().get(0),
        // PostgresDataType.DECIMAL.get()));
        // PostgresAggregate sum = new PostgresAggregate(PostgresAggregateFunction.SUM,
        // aggregate.getExpr());
        // PostgresCast count = new PostgresCast(
        // new PostgresAggregate(PostgresAggregateFunction.COUNT, aggregate.getExpr()),
        // PostgresDataType.DECIMAL.get());
        //// PostgresBinaryArithmeticOperation avg = new
        // PostgresBinaryArithmeticOperation(sum, count,
        // PostgresBinaryArithmeticOperator.DIV);
        // return aliasArgs(Arrays.asList(sum, count));
        default:
            throw new AssertionError(aggregate.getFunction());
        }
    }

    private List<PostgresExpression> aliasArgs(List<PostgresExpression> originalAggregateArgs) {
        List<PostgresExpression> args = new ArrayList<>();
        int i = 0;
        for (PostgresExpression expr : originalAggregateArgs) {
            args.add(new PostgresAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(PostgresAggregate aggregate) {
        switch (aggregate.getFunction()) {
        // case AVG:
        // return "SUM(agg0::DECIMAL)/SUM(agg1)::DECIMAL";
        case COUNT:
            return PostgresAggregateFunction.SUM.toString() + "(agg0)";
        default:
            return aggregate.getFunction().toString() + "(agg0)";
        }
    }

    private PostgresSelect getSelect(List<PostgresExpression> aggregates, List<PostgresExpression> from,
            PostgresExpression whereClause, List<PostgresJoin> joinList) {
        PostgresSelect leftSelect = new PostgresSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinClauses(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
