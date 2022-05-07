package DT2.postgres;

import java.util.List;

import DT2.postgres.PostgresSchema.PostgresColumn;
import DT2.postgres.PostgresSchema.PostgresDataType;
import DT2.postgres.ast.PostgresAggregate;
import DT2.postgres.ast.PostgresBetweenOperation;
import DT2.postgres.ast.PostgresBinaryLogicalOperation;
import DT2.postgres.ast.PostgresCastOperation;
import DT2.postgres.ast.PostgresCollate;
import DT2.postgres.ast.PostgresColumnValue;
import DT2.postgres.ast.PostgresConstant;
import DT2.postgres.ast.PostgresExpression;
import DT2.postgres.ast.PostgresFunction;
import DT2.postgres.ast.PostgresInOperation;
import DT2.postgres.ast.PostgresLikeOperation;
import DT2.postgres.ast.PostgresOrderByTerm;
import DT2.postgres.ast.PostgresPOSIXRegularExpression;
import DT2.postgres.ast.PostgresPostfixOperation;
import DT2.postgres.ast.PostgresPostfixText;
import DT2.postgres.ast.PostgresPrefixOperation;
import DT2.postgres.ast.PostgresSelect;
import DT2.postgres.ast.PostgresSelect.PostgresFromTable;
import DT2.postgres.ast.PostgresSelect.PostgresSubquery;
import DT2.postgres.ast.PostgresSimilarTo;
import DT2.postgres.gen.PostgresExpressionGenerator;

public interface PostgresVisitor {

    void visit(PostgresConstant constant);

    void visit(PostgresPostfixOperation op);

    void visit(PostgresColumnValue c);

    void visit(PostgresPrefixOperation op);

    void visit(PostgresSelect op);

    void visit(PostgresOrderByTerm op);

    void visit(PostgresFunction f);

    void visit(PostgresCastOperation cast);

    void visit(PostgresBetweenOperation op);

    void visit(PostgresInOperation op);

    void visit(PostgresPostfixText op);

    void visit(PostgresAggregate op);

    void visit(PostgresSimilarTo op);

    void visit(PostgresCollate op);

    void visit(PostgresPOSIXRegularExpression op);

    void visit(PostgresFromTable from);

    void visit(PostgresSubquery subquery);

    void visit(PostgresBinaryLogicalOperation op);

    void visit(PostgresLikeOperation op);

    default void visit(PostgresExpression expression) {
        if (expression instanceof PostgresConstant) {
            visit((PostgresConstant) expression);
        } else if (expression instanceof PostgresPostfixOperation) {
            visit((PostgresPostfixOperation) expression);
        } else if (expression instanceof PostgresColumnValue) {
            visit((PostgresColumnValue) expression);
        } else if (expression instanceof PostgresPrefixOperation) {
            visit((PostgresPrefixOperation) expression);
        } else if (expression instanceof PostgresSelect) {
            visit((PostgresSelect) expression);
        } else if (expression instanceof PostgresOrderByTerm) {
            visit((PostgresOrderByTerm) expression);
        } else if (expression instanceof PostgresFunction) {
            visit((PostgresFunction) expression);
        } else if (expression instanceof PostgresCastOperation) {
            visit((PostgresCastOperation) expression);
        } else if (expression instanceof PostgresBetweenOperation) {
            visit((PostgresBetweenOperation) expression);
        } else if (expression instanceof PostgresInOperation) {
            visit((PostgresInOperation) expression);
        } else if (expression instanceof PostgresAggregate) {
            visit((PostgresAggregate) expression);
        } else if (expression instanceof PostgresPostfixText) {
            visit((PostgresPostfixText) expression);
        } else if (expression instanceof PostgresSimilarTo) {
            visit((PostgresSimilarTo) expression);
        } else if (expression instanceof PostgresPOSIXRegularExpression) {
            visit((PostgresPOSIXRegularExpression) expression);
        } else if (expression instanceof PostgresCollate) {
            visit((PostgresCollate) expression);
        } else if (expression instanceof PostgresFromTable) {
            visit((PostgresFromTable) expression);
        } else if (expression instanceof PostgresSubquery) {
            visit((PostgresSubquery) expression);
        } else if (expression instanceof PostgresLikeOperation) {
            visit((PostgresLikeOperation) expression);
        } else {
            throw new AssertionError(expression);
        }
    }

    static String asString(PostgresExpression expr) {
        PostgresToStringVisitor visitor = new PostgresToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(PostgresExpression expr) {
        PostgresExpectedValueVisitor v = new PostgresExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

    static String getExpressionAsString(PostgresGlobalState globalState, PostgresDataType type,
            List<PostgresColumn> columns) {
        PostgresExpression expression = PostgresExpressionGenerator.generateExpression(globalState, columns, type);
        PostgresToStringVisitor visitor = new PostgresToStringVisitor();
        visitor.visit(expression);
        return visitor.get();
    }

}
