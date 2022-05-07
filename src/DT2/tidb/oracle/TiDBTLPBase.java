package DT2.tidb.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.gen.ExpressionGenerator;
import DT2.common.oracle.TernaryLogicPartitioningOracleBase;
import DT2.common.oracle.TestOracle;
import DT2.tidb.TiDBErrors;
import DT2.tidb.TiDBExpressionGenerator;
import DT2.tidb.TiDBProvider.TiDBGlobalState;
import DT2.tidb.TiDBSchema;
import DT2.tidb.TiDBSchema.TiDBTable;
import DT2.tidb.TiDBSchema.TiDBTables;
import DT2.tidb.ast.TiDBColumnReference;
import DT2.tidb.ast.TiDBExpression;
import DT2.tidb.ast.TiDBJoin;
import DT2.tidb.ast.TiDBSelect;
import DT2.tidb.ast.TiDBTableReference;
import DT2.tidb.gen.TiDBHintGenerator;

public abstract class TiDBTLPBase extends TernaryLogicPartitioningOracleBase<TiDBExpression, TiDBGlobalState>
        implements TestOracle {

    TiDBSchema s;
    TiDBTables targetTables;
    TiDBExpressionGenerator gen;
    TiDBSelect select;

    public TiDBTLPBase(TiDBGlobalState state) {
        super(state);
        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new TiDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new TiDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<TiDBTable> tables = targetTables.getTables();
        if (Randomly.getBoolean()) {
            TiDBHintGenerator.generateHints(select, tables);
        }

        List<TiDBExpression> tableList = tables.stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        List<TiDBExpression> joins = TiDBJoin.getJoins(tableList, state);
        select.setJoinList(joins);
        select.setFromList(tableList);
        select.setWhereClause(null);
    }

    List<TiDBExpression> generateFetchColumns() {
        return Arrays.asList(new TiDBColumnReference(targetTables.getColumns().get(0)));
    }

    @Override
    protected ExpressionGenerator<TiDBExpression> getGen() {
        return gen;
    }

}
