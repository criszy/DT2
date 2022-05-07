package DT2.oceanbase.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import DT2.common.gen.ExpressionGenerator;
import DT2.common.oracle.TernaryLogicPartitioningOracleBase;
import DT2.common.oracle.TestOracle;
import DT2.oceanbase.OceanBaseErrors;
import DT2.oceanbase.OceanBaseGlobalState;
import DT2.oceanbase.OceanBaseSchema;
import DT2.oceanbase.OceanBaseSchema.OceanBaseTable;
import DT2.oceanbase.OceanBaseSchema.OceanBaseTables;
import DT2.oceanbase.ast.OceanBaseColumnReference;
import DT2.oceanbase.ast.OceanBaseExpression;
import DT2.oceanbase.ast.OceanBaseSelect;
import DT2.oceanbase.ast.OceanBaseTableReference;
import DT2.oceanbase.gen.OceanBaseExpressionGenerator;
import DT2.oceanbase.gen.OceanBaseHintGenerator;

public abstract class OceanBaseTLPBase
        extends TernaryLogicPartitioningOracleBase<OceanBaseExpression, OceanBaseGlobalState> implements TestOracle {

    OceanBaseSchema s;
    OceanBaseTables targetTables;
    OceanBaseExpressionGenerator gen;
    OceanBaseSelect select;

    public OceanBaseTLPBase(OceanBaseGlobalState state) {
        super(state);
        OceanBaseErrors.addExpressionErrors(errors);
        errors.add("value is out of range");
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new OceanBaseExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new OceanBaseSelect();
        select.setFetchColumns(generateFetchColumns());
        List<OceanBaseTable> tables = targetTables.getTables();
        OceanBaseHintGenerator.generateHints(select, tables);
        List<OceanBaseExpression> tableList = tables.stream().map(t -> new OceanBaseTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);
        select.setWhereClause(null);
    }

    List<OceanBaseExpression> generateFetchColumns() {
        return Arrays.asList(OceanBaseColumnReference.create(targetTables.getColumns().get(0), null));
    }

    @Override
    protected ExpressionGenerator<OceanBaseExpression> getGen() {
        return gen;
    }

}
