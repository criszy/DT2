package DT2.mongodb.gen;

import java.util.ArrayList;
import java.util.List;

import DT2.Randomly;
import DT2.common.ast.newast.NewFunctionNode;
import DT2.common.ast.newast.Node;
import DT2.common.gen.UntypedExpressionGenerator;
import DT2.mongodb.MongoDBProvider.MongoDBGlobalState;
import DT2.mongodb.MongoDBSchema;
import DT2.mongodb.ast.MongoDBExpression;
import DT2.mongodb.test.MongoDBColumnTestReference;

public class MongoDBComputedExpressionGenerator
        extends UntypedExpressionGenerator<Node<MongoDBExpression>, MongoDBColumnTestReference> {

    private final MongoDBGlobalState globalState;

    @Override
    public Node<MongoDBExpression> generateLeafNode() {
        ComputedFunction function = ComputedFunction.getRandom();
        List<Node<MongoDBExpression>> expressions = new ArrayList<>();
        for (int i = 0; i < function.getNrArgs(); i++) {
            expressions.add(super.generateLeafNode());
        }
        return new NewFunctionNode<>(expressions, function);
    }

    @Override
    protected Node<MongoDBExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        ComputedFunction func = ComputedFunction.getRandom();
        return new NewFunctionNode<>(generateExpressions(depth + 1, func.getNrArgs()), func);
    }

    public MongoDBComputedExpressionGenerator(MongoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public enum ComputedFunction {
        ADD(2, "$add"), MULTIPLY(2, "$multiply"), DIVIDE(2, "$divide"), POW(2, "$pow"), SQRT(1, "$sqrt"),
        LOG(2, "$log"), AVG(2, "$avg"), EXP(1, "$exp");

        private final int nrArgs;
        private final String operatorName;

        ComputedFunction(int nrArgs, String operatorName) {
            this.nrArgs = nrArgs;
            this.operatorName = operatorName;
        }

        public static ComputedFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

        public String getOperator() {
            return operatorName;
        }
    }

    @Override
    public Node<MongoDBExpression> generateConstant() {
        MongoDBSchema.MongoDBDataType type = MongoDBSchema.MongoDBDataType.getRandom(globalState);
        MongoDBConstantGenerator generator = new MongoDBConstantGenerator(globalState);
        return generator.generateConstantWithType(type);
    }

    @Override
    protected Node<MongoDBExpression> generateColumn() {
        return Randomly.fromList(columns);
    }

    @Override
    public Node<MongoDBExpression> negatePredicate(Node<MongoDBExpression> predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node<MongoDBExpression> isNull(Node<MongoDBExpression> expr) {
        throw new UnsupportedOperationException();
    }
}
