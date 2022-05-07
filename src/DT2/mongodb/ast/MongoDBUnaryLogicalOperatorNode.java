package DT2.mongodb.ast;

import DT2.common.ast.newast.NewUnaryPrefixOperatorNode;
import DT2.common.ast.newast.Node;
import DT2.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBUnaryLogicalOperator;

public class MongoDBUnaryLogicalOperatorNode extends NewUnaryPrefixOperatorNode<MongoDBExpression> {

    public MongoDBUnaryLogicalOperatorNode(Node<MongoDBExpression> expr, MongoDBUnaryLogicalOperator op) {
        super(expr, op);
    }

    public MongoDBUnaryLogicalOperator operator() {
        return (MongoDBUnaryLogicalOperator) op;
    }
}
