package DT2.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.clickhouse.ClickHouseErrors;
import DT2.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import DT2.clickhouse.ClickHouseSchema;
import DT2.clickhouse.ClickHouseSchema.ClickHouseTable;
import DT2.clickhouse.ClickHouseSchema.ClickHouseTables;
import DT2.clickhouse.ast.ClickHouseColumnReference;
import DT2.clickhouse.ast.ClickHouseExpression;
import DT2.clickhouse.ast.ClickHouseExpression.ClickHouseJoin;
import DT2.clickhouse.ast.ClickHouseSelect;
import DT2.clickhouse.gen.ClickHouseCommon;
import DT2.clickhouse.gen.ClickHouseExpressionGenerator;
import DT2.common.gen.ExpressionGenerator;
import DT2.common.oracle.TernaryLogicPartitioningOracleBase;
import DT2.common.oracle.TestOracle;

public class ClickHouseTLPBase extends TernaryLogicPartitioningOracleBase<ClickHouseExpression, ClickHouseGlobalState>
        implements TestOracle {

    ClickHouseSchema s;
    ClickHouseTables targetTables;
    ClickHouseExpressionGenerator gen;
    ClickHouseSelect select;

    public ClickHouseTLPBase(ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(errors);
        ClickHouseErrors.addQueryErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new ClickHouseExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new ClickHouseSelect();
        select.setFetchColumns(generateFetchColumns());
        List<ClickHouseTable> tables = targetTables.getTables();
        List<ClickHouseJoin> joinStatements = gen.getRandomJoinClauses(tables);
        List<ClickHouseExpression> tableRefs = ClickHouseCommon.getTableRefs(tables, s);
        select.setJoinClauses(joinStatements.stream().collect(Collectors.toList()));
        select.setFromTables(tableRefs);
        select.setWhereClause(null);
    }

    List<ClickHouseExpression> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ClickHouseColumnReference(c, null)).collect(Collectors.toList());
    }

    @Override
    protected ExpressionGenerator<ClickHouseExpression> getGen() {
        return gen;
    }

}
