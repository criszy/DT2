package DT2.mongodb.ast;

import DT2.common.ast.newast.NewBinaryOperatorNode;
import DT2.common.ast.newast.Node;
import DT2.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBBinaryLogicalOperator;

public class MongoDBBinaryLogicalNode extends NewBinaryOperatorNode<MongoDBExpression> {
    public MongoDBBinaryLogicalNode(Node<MongoDBExpression> left, Node<MongoDBExpression> right,
            MongoDBBinaryLogicalOperator op) {
        super(left, right, op);
    }

    public MongoDBBinaryLogicalOperator operator() {
        return (MongoDBBinaryLogicalOperator) op;
    }
}
