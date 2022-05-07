package DT2.mongodb.ast;

import static DT2.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBRegexOperator.REGEX;

import DT2.common.ast.newast.NewBinaryOperatorNode;
import DT2.common.ast.newast.Node;
import DT2.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBRegexOperator;

public class MongoDBRegexNode extends NewBinaryOperatorNode<MongoDBExpression> {
    private final String options;

    public MongoDBRegexNode(Node<MongoDBExpression> left, Node<MongoDBExpression> right, String options) {
        super(left, right, REGEX);
        this.options = options;
    }

    public String getOptions() {
        return options;
    }

    public MongoDBRegexOperator operator() {
        return (MongoDBRegexOperator) op;
    }
}
