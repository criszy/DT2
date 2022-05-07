package DT2.mongodb.ast;

import DT2.common.ast.newast.NewBinaryOperatorNode;
import DT2.common.ast.newast.Node;
import DT2.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBBinaryComparisonOperator;

public class MongoDBBinaryComparisonNode extends NewBinaryOperatorNode<MongoDBExpression> {
    public MongoDBBinaryComparisonNode(Node<MongoDBExpression> left, Node<MongoDBExpression> right,
            MongoDBBinaryComparisonOperator op) {
        super(left, right, op);
    }

    public MongoDBBinaryComparisonOperator operator() {
        return (MongoDBBinaryComparisonOperator) op;
    }
}
