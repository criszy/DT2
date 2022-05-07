package DT2.arangodb.visitor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import DT2.arangodb.ArangoDBSchema;
import DT2.arangodb.ast.ArangoDBConstant;
import DT2.arangodb.ast.ArangoDBExpression;
import DT2.arangodb.ast.ArangoDBSelect;
import DT2.arangodb.gen.ArangoDBComputedExpressionGenerator;
import DT2.arangodb.query.ArangoDBSelectQuery;
import DT2.common.ast.newast.ColumnReferenceNode;
import DT2.common.ast.newast.NewBinaryOperatorNode;
import DT2.common.ast.newast.NewFunctionNode;
import DT2.common.ast.newast.NewUnaryPrefixOperatorNode;
import DT2.common.ast.newast.Node;

public class ArangoDBToQueryVisitor extends ArangoDBVisitor<ArangoDBExpression> {

    private final StringBuilder stringBuilder;

    public ArangoDBToQueryVisitor() {
        stringBuilder = new StringBuilder();
    }

    @Override
    protected void visit(ArangoDBSelect<ArangoDBExpression> expression) {
        generateFrom(expression);
        generateComputed(expression);
        generateFilter(expression);
        generateProject(expression);
    }

    private void generateFilter(ArangoDBSelect<ArangoDBExpression> expression) {
        if (expression.hasFilter()) {
            stringBuilder.append("FILTER ");
            visit(expression.getFilterClause());
            stringBuilder.append(" ");
        }
    }

    private void generateComputed(ArangoDBSelect<ArangoDBExpression> expression) {
        if (expression.hasComputed()) {
            List<Node<ArangoDBExpression>> computedClause = expression.getComputedClause();
            int computedNumber = 0;
            for (Node<ArangoDBExpression> computedExpression : computedClause) {
                stringBuilder.append("LET c").append(computedNumber).append(" = ");
                visit(computedExpression);
                stringBuilder.append(" ");
                computedNumber++;
            }
        }
    }

    @Override
    protected void visit(ColumnReferenceNode<ArangoDBExpression, ?> expression) {
        if (expression.getColumn().getTable().getName().equals("")) {
            stringBuilder.append(expression.getColumn().getName());
        } else {
            stringBuilder.append("r").append(expression.getColumn().getTable().getName()).append(".")
                    .append(expression.getColumn().getName());
        }
    }

    @Override
    protected void visit(ArangoDBConstant expression) {
        stringBuilder.append(expression.getValue());
    }

    @Override
    protected void visit(NewBinaryOperatorNode<ArangoDBExpression> expression) {
        stringBuilder.append("(");
        visit(expression.getLeft());
        stringBuilder.append(" ").append(expression.getOperatorRepresentation()).append(" ");
        visit(expression.getRight());
        stringBuilder.append(")");
    }

    @Override
    protected void visit(NewUnaryPrefixOperatorNode<ArangoDBExpression> expression) {
        stringBuilder.append(expression.getOperatorRepresentation()).append("(");
        visit(expression.getExpr());
        stringBuilder.append(")");
    }

    @Override
    protected void visit(NewFunctionNode<ArangoDBExpression, ?> expression) {
        if (!(expression.getFunc() instanceof ArangoDBComputedExpressionGenerator.ComputedFunction)) {
            throw new UnsupportedOperationException();
        }
        ArangoDBComputedExpressionGenerator.ComputedFunction function = (ArangoDBComputedExpressionGenerator.ComputedFunction) expression
                .getFunc();
        // TODO: Support functions with a different number of arguments.
        if (function.getNrArgs() != 2) {
            throw new UnsupportedOperationException();
        }
        stringBuilder.append("(");
        visit(expression.getArgs().get(0));
        stringBuilder.append(" ").append(function.getOperatorName()).append(" ");
        visit(expression.getArgs().get(1));
        stringBuilder.append(")");
    }

    private void generateFrom(ArangoDBSelect<ArangoDBExpression> expression) {
        List<ArangoDBSchema.ArangoDBColumn> forColumns = expression.getFromColumns();
        Set<ArangoDBSchema.ArangoDBTable> tables = new HashSet<>();
        for (ArangoDBSchema.ArangoDBColumn column : forColumns) {
            tables.add(column.getTable());
        }

        for (ArangoDBSchema.ArangoDBTable table : tables) {
            stringBuilder.append("FOR r").append(table.getName()).append(" IN ").append(table.getName()).append(" ");
        }
    }

    private void generateProject(ArangoDBSelect<ArangoDBExpression> expression) {
        List<ArangoDBSchema.ArangoDBColumn> projectColumns = expression.getProjectionColumns();
        stringBuilder.append("RETURN {");
        String filler = "";
        for (ArangoDBSchema.ArangoDBColumn column : projectColumns) {
            stringBuilder.append(filler);
            filler = ", ";
            stringBuilder.append(column.getTable().getName()).append("_").append(column.getName()).append(": r")
                    .append(column.getTable().getName()).append(".").append(column.getName());
        }
        stringBuilder.append("}");
    }

    public ArangoDBSelectQuery getQuery() {
        return new ArangoDBSelectQuery(stringBuilder.toString());
    }

}
