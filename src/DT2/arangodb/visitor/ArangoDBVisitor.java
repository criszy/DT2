package DT2.arangodb.visitor;

import DT2.arangodb.ast.ArangoDBConstant;
import DT2.arangodb.ast.ArangoDBExpression;
import DT2.arangodb.ast.ArangoDBSelect;
import DT2.arangodb.query.ArangoDBSelectQuery;
import DT2.common.ast.newast.ColumnReferenceNode;
import DT2.common.ast.newast.NewBinaryOperatorNode;
import DT2.common.ast.newast.NewFunctionNode;
import DT2.common.ast.newast.NewUnaryPrefixOperatorNode;
import DT2.common.ast.newast.Node;

public abstract class ArangoDBVisitor<E> {

    protected abstract void visit(ArangoDBSelect<E> expression);

    protected abstract void visit(ColumnReferenceNode<E, ?> expression);

    protected abstract void visit(ArangoDBConstant expression);

    protected abstract void visit(NewBinaryOperatorNode<E> expression);

    protected abstract void visit(NewUnaryPrefixOperatorNode<E> expression);

    protected abstract void visit(NewFunctionNode<E, ?> expression);

    @SuppressWarnings("unchecked")
    public void visit(Node<E> expressionNode) {
        if (expressionNode instanceof ArangoDBSelect) {
            visit((ArangoDBSelect<E>) expressionNode);
        } else if (expressionNode instanceof ColumnReferenceNode<?, ?>) {
            visit((ColumnReferenceNode<E, ?>) expressionNode);
        } else if (expressionNode instanceof ArangoDBConstant) {
            visit((ArangoDBConstant) expressionNode);
        } else if (expressionNode instanceof NewBinaryOperatorNode<?>) {
            visit((NewBinaryOperatorNode<E>) expressionNode);
        } else if (expressionNode instanceof NewUnaryPrefixOperatorNode<?>) {
            visit((NewUnaryPrefixOperatorNode<E>) expressionNode);
        } else if (expressionNode instanceof NewFunctionNode<?, ?>) {
            visit((NewFunctionNode<E, ?>) expressionNode);
        } else {
            throw new AssertionError(expressionNode);
        }
    }

    public static ArangoDBSelectQuery asSelectQuery(Node<ArangoDBExpression> expressionNode) {
        ArangoDBToQueryVisitor visitor = new ArangoDBToQueryVisitor();
        visitor.visit(expressionNode);
        return visitor.getQuery();
    }
}
