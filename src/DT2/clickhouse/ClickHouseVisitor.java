package DT2.clickhouse;

import DT2.clickhouse.ast.ClickHouseAggregate;
import DT2.clickhouse.ast.ClickHouseBinaryComparisonOperation;
import DT2.clickhouse.ast.ClickHouseBinaryLogicalOperation;
import DT2.clickhouse.ast.ClickHouseCastOperation;
import DT2.clickhouse.ast.ClickHouseColumnReference;
import DT2.clickhouse.ast.ClickHouseConstant;
import DT2.clickhouse.ast.ClickHouseExpression;
import DT2.clickhouse.ast.ClickHouseSelect;
import DT2.clickhouse.ast.ClickHouseTableReference;
import DT2.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import DT2.clickhouse.ast.ClickHouseUnaryPrefixOperation;

public interface ClickHouseVisitor {
    // TODO remove these default methods

    default void visit(ClickHouseBinaryComparisonOperation op) {

    }

    default void visit(ClickHouseBinaryLogicalOperation op) {

    }

    default void visit(ClickHouseUnaryPrefixOperation exp) {

    }

    default void visit(ClickHouseUnaryPostfixOperation op) {

    }

    default void visit(ClickHouseConstant c) {

    }

    default void visit(ClickHouseSelect s, boolean inner) {

    };

    default void visit(ClickHouseColumnReference columnReference) {

    };

    default void visit(ClickHouseExpression.ClickHousePostfixText op) {

    }

    void visit(ClickHouseTableReference tableReference);

    void visit(ClickHouseCastOperation cast);

    void visit(ClickHouseExpression.ClickHouseJoin join);

    void visit(ClickHouseAggregate aggregate);

    default void visit(ClickHouseExpression expr) {
        if (expr instanceof ClickHouseBinaryComparisonOperation) {
            visit((ClickHouseBinaryComparisonOperation) expr);
        } else if (expr instanceof ClickHouseBinaryLogicalOperation) {
            visit((ClickHouseBinaryLogicalOperation) expr);
        } else if (expr instanceof ClickHouseConstant) {
            visit((ClickHouseConstant) expr);
        } else if (expr instanceof ClickHouseUnaryPrefixOperation) {
            visit((ClickHouseUnaryPrefixOperation) expr);
        } else if (expr instanceof ClickHouseSelect) {
            visit((ClickHouseSelect) expr, true);
        } else if (expr instanceof ClickHouseColumnReference) {
            visit((ClickHouseColumnReference) expr);
        } else if (expr instanceof ClickHouseTableReference) {
            visit((ClickHouseTableReference) expr);
        } else if (expr instanceof ClickHouseCastOperation) {
            visit((ClickHouseCastOperation) expr);
        } else if (expr instanceof ClickHouseExpression.ClickHouseJoin) {
            visit((ClickHouseExpression.ClickHouseJoin) expr);
        } else if (expr instanceof ClickHouseExpression.ClickHousePostfixText) {
            visit((ClickHouseExpression.ClickHousePostfixText) expr);
        } else if (expr instanceof ClickHouseAggregate) {
            visit((ClickHouseAggregate) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(ClickHouseExpression expr) {
        if (expr == null) {
            throw new AssertionError();
        }
        ClickHouseToStringVisitor visitor = new ClickHouseToStringVisitor();
        if (expr instanceof ClickHouseSelect) {
            visitor.visit((ClickHouseSelect) expr, false);
        } else {
            visitor.visit(expr);
        }
        return visitor.get();
    }

}