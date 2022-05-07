package DT2.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.gen.ExpressionGenerator;
import DT2.common.oracle.TernaryLogicPartitioningOracleBase;
import DT2.common.oracle.TestOracle;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.PostgresSchema;
import DT2.postgres.PostgresSchema.PostgresColumn;
import DT2.postgres.PostgresSchema.PostgresDataType;
import DT2.postgres.PostgresSchema.PostgresTable;
import DT2.postgres.PostgresSchema.PostgresTables;
import DT2.postgres.ast.PostgresColumnValue;
import DT2.postgres.ast.PostgresConstant;
import DT2.postgres.ast.PostgresExpression;
import DT2.postgres.ast.PostgresJoin;
import DT2.postgres.ast.PostgresSelect;
import DT2.postgres.ast.PostgresSelect.ForClause;
import DT2.postgres.ast.PostgresSelect.PostgresFromTable;
import DT2.postgres.ast.PostgresSelect.PostgresSubquery;
import DT2.postgres.gen.PostgresCommon;
import DT2.postgres.gen.PostgresExpressionGenerator;
import DT2.postgres.oracle.PostgresNoRECOracle;

public class PostgresTLPBase extends TernaryLogicPartitioningOracleBase<PostgresExpression, PostgresGlobalState>
        implements TestOracle {

    protected PostgresSchema s;
    protected PostgresTables targetTables;
    protected PostgresExpressionGenerator gen;
    protected PostgresSelect select;

    public PostgresTLPBase(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonFetchErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        List<PostgresTable> tables = targetTables.getTables();
        List<PostgresJoin> joins = getJoinStatements(state, targetTables.getColumns(), tables);
        generateSelectBase(tables, joins);
    }

    protected List<PostgresJoin> getJoinStatements(PostgresGlobalState globalState, List<PostgresColumn> columns,
            List<PostgresTable> tables) {
        return PostgresNoRECOracle.getJoinStatements(state, columns, tables);
        // TODO joins
    }

    protected void generateSelectBase(List<PostgresTable> tables, List<PostgresJoin> joins) {
        List<PostgresExpression> tableList = tables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        gen = new PostgresExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new PostgresSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setFromList(tableList);
        select.setWhereClause(null);
        select.setJoinClauses(joins);
        if (Randomly.getBoolean()) {
            select.setForClause(ForClause.getRandom());
        }
    }

    List<PostgresExpression> generateFetchColumns() {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return Arrays.asList(new PostgresColumnValue(PostgresColumn.createDummy("*"), null));
        }
        List<PostgresExpression> fetchColumns = new ArrayList<>();
        List<PostgresColumn> targetColumns = Randomly.nonEmptySubset(targetTables.getColumns());
        for (PostgresColumn c : targetColumns) {
            fetchColumns.add(new PostgresColumnValue(c, null));
        }
        return fetchColumns;
    }

    @Override
    protected ExpressionGenerator<PostgresExpression> getGen() {
        return gen;
    }

    public static PostgresSubquery createSubquery(PostgresGlobalState globalState, String name, PostgresTables tables) {
        List<PostgresExpression> columns = new ArrayList<>();
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            columns.add(gen.generateExpression(0));
        }
        PostgresSelect select = new PostgresSelect();
        select.setFromList(tables.getTables().stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, PostgresDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(
                        PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setForClause(ForClause.getRandom());
        }
        return new PostgresSubquery(select, name);
    }

}
