package DT2.arangodb.test;

import java.util.ArrayList;
import java.util.List;

import DT2.Randomly;
import DT2.arangodb.ArangoDBProvider;
import DT2.arangodb.ArangoDBSchema;
import DT2.arangodb.ast.ArangoDBExpression;
import DT2.arangodb.ast.ArangoDBSelect;
import DT2.arangodb.gen.ArangoDBComputedExpressionGenerator;
import DT2.arangodb.gen.ArangoDBFilterExpressionGenerator;
import DT2.common.ast.newast.Node;
import DT2.common.gen.ExpressionGenerator;
import DT2.common.oracle.TernaryLogicPartitioningOracleBase;
import DT2.common.oracle.TestOracle;

public class ArangoDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<ArangoDBExpression>, ArangoDBProvider.ArangoDBGlobalState>
        implements TestOracle {

    protected ArangoDBSchema schema;
    protected List<ArangoDBSchema.ArangoDBColumn> targetColumns;
    protected ArangoDBFilterExpressionGenerator expressionGenerator;
    protected ArangoDBSelect<ArangoDBExpression> select;
    protected int numberComputedColumns;

    protected ArangoDBQueryPartitioningBase(ArangoDBProvider.ArangoDBGlobalState state) {
        super(state);
    }

    @Override
    protected ExpressionGenerator<Node<ArangoDBExpression>> getGen() {
        return expressionGenerator;
    }

    @Override
    public void check() throws Exception {
        numberComputedColumns = state.getRandomly().getInteger(0, 4);
        schema = state.getSchema();
        generateTargetColumns();
        expressionGenerator = new ArangoDBFilterExpressionGenerator(state).setColumns(targetColumns);
        expressionGenerator.setNumberOfComputedVariables(numberComputedColumns);
        initializeTernaryPredicateVariants();
        select = new ArangoDBSelect<>();
        select.setFromColumns(targetColumns);
        select.setProjectionColumns(Randomly.nonEmptySubset(targetColumns));
        generateComputedClause();
    }

    private void generateComputedClause() {
        List<Node<ArangoDBExpression>> computedColumns = new ArrayList<>();
        ArangoDBComputedExpressionGenerator generator = new ArangoDBComputedExpressionGenerator(state);
        generator.setColumns(targetColumns);
        for (int i = 0; i < numberComputedColumns; i++) {
            computedColumns.add(generator.generateExpression());
        }
        select.setComputedClause(computedColumns);
    }

    private void generateTargetColumns() {
        ArangoDBSchema.ArangoDBTables targetTables;
        targetTables = schema.getRandomTableNonEmptyTables();
        List<ArangoDBSchema.ArangoDBColumn> allColumns = targetTables.getColumns();
        targetColumns = Randomly.nonEmptySubset(allColumns);
    }
}
