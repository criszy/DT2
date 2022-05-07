package DT2.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import DT2.ComparatorHelper;
import DT2.Randomly;
import DT2.clickhouse.ClickHouseErrors;
import DT2.clickhouse.ClickHouseProvider;
import DT2.clickhouse.ClickHouseSchema;
import DT2.clickhouse.ClickHouseVisitor;
import DT2.clickhouse.ast.ClickHouseColumnReference;
import DT2.clickhouse.ast.ClickHouseExpression;
import DT2.clickhouse.ast.ClickHouseSelect;
import DT2.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import DT2.clickhouse.ast.ClickHouseUnaryPrefixOperation;
import DT2.clickhouse.gen.ClickHouseCommon;
import DT2.clickhouse.gen.ClickHouseExpressionGenerator;

public class ClickHouseTLPHavingOracle extends ClickHouseTLPBase {

    public ClickHouseTLPHavingOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(errors);
        ClickHouseErrors.addGroupingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        ClickHouseSchema s = state.getSchema();
        ClickHouseSchema.ClickHouseTables targetTables = s.getRandomTableNonEmptyTables();
        List<ClickHouseExpression> groupByColumns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ClickHouseColumnReference(c, null)).collect(Collectors.toList());
        List<ClickHouseSchema.ClickHouseColumn> columns = targetTables.getColumns();
        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(state).setColumns(columns);
        ClickHouseExpressionGenerator aggrGen = new ClickHouseExpressionGenerator(state).allowAggregates(true)
                .setColumns(columns);
        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(aggrGen.generateExpressions(Randomly.smallNumber() + 1));
        List<ClickHouseSchema.ClickHouseTable> tables = targetTables.getTables();
        List<ClickHouseExpression.ClickHouseJoin> joinStatements = gen.getRandomJoinClauses(tables);
        List<ClickHouseExpression> from = ClickHouseCommon.getTableRefs(tables, state.getSchema());
        select.setJoinClauses(joinStatements);
        select.setSelectType(ClickHouseSelect.SelectType.ALL);
        select.setFromTables(from);
        // TODO order by?
        select.setGroupByClause(groupByColumns);
        select.setHavingClause(null);
        String originalQueryString = ClickHouseVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        ClickHouseExpression predicate = aggrGen.getHavingClause();
        select.setHavingClause(predicate);
        String firstQueryString = ClickHouseVisitor.asString(select);
        select.setHavingClause(new ClickHouseUnaryPrefixOperation(predicate,
                ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator.NOT));
        String secondQueryString = ClickHouseVisitor.asString(select);
        select.setHavingClause(new ClickHouseUnaryPostfixOperation(predicate,
                ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator.IS_NULL, false));
        String thirdQueryString = ClickHouseVisitor.asString(select);
        String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
        combinedString += " SETTINGS aggregate_functions_null_for_empty=1, enable_optimize_predicate_expression=0"; // https://github.com/ClickHouse/ClickHouse/issues/12264
        List<String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(combinedString, errors, state);
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(originalQueryString);
            state.getLogger().writeCurrent(combinedString);
        }
        if (new HashSet<>(resultSet).size() != new HashSet<>(secondResultSet).size()) {
            HashSet<String> diffLeft = new HashSet<>(resultSet);
            HashSet<String> tmpLeft = new HashSet<>(resultSet);
            HashSet<String> diffRight = new HashSet<>(secondResultSet);
            diffLeft.removeAll(diffRight);
            diffRight.removeAll(tmpLeft);
            throw new AssertionError(originalQueryString + ";\n" + combinedString + ";\n" + "Left: "
                    + diffLeft.toString() + "\nRight: " + diffRight.toString());
        }
    }
}
