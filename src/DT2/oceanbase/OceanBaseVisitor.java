package DT2.oceanbase;

import DT2.oceanbase.ast.OceanBaseAggregate;
import DT2.oceanbase.ast.OceanBaseBinaryComparisonOperation;
import DT2.oceanbase.ast.OceanBaseBinaryLogicalOperation;
import DT2.oceanbase.ast.OceanBaseCastOperation;
import DT2.oceanbase.ast.OceanBaseColumnName;
import DT2.oceanbase.ast.OceanBaseColumnReference;
import DT2.oceanbase.ast.OceanBaseComputableFunction;
import DT2.oceanbase.ast.OceanBaseConstant;
import DT2.oceanbase.ast.OceanBaseExists;
import DT2.oceanbase.ast.OceanBaseExpression;
import DT2.oceanbase.ast.OceanBaseInOperation;
import DT2.oceanbase.ast.OceanBaseOrderByTerm;
import DT2.oceanbase.ast.OceanBaseSelect;
import DT2.oceanbase.ast.OceanBaseStringExpression;
import DT2.oceanbase.ast.OceanBaseTableReference;
import DT2.oceanbase.ast.OceanBaseText;
import DT2.oceanbase.ast.OceanBaseUnaryPostfixOperation;
import DT2.oceanbase.ast.OceanBaseUnaryPrefixOperation;

public interface OceanBaseVisitor {

    void visit(OceanBaseTableReference ref);

    void visit(OceanBaseConstant constant);

    void visit(OceanBaseColumnReference column);

    void visit(OceanBaseUnaryPostfixOperation column);

    void visit(OceanBaseComputableFunction f);

    void visit(OceanBaseBinaryLogicalOperation op);

    void visit(OceanBaseSelect select);

    void visit(OceanBaseBinaryComparisonOperation op);

    void visit(OceanBaseCastOperation op);

    void visit(OceanBaseInOperation op);

    void visit(OceanBaseOrderByTerm op);

    void visit(OceanBaseExists op);

    void visit(OceanBaseStringExpression op);

    void visit(OceanBaseAggregate aggr);

    void visit(OceanBaseColumnName c);

    void visit(OceanBaseText fun);

    void visit(OceanBaseUnaryPrefixOperation op);

    default void visit(OceanBaseExpression expr) {
        if (expr instanceof OceanBaseConstant) {
            visit((OceanBaseConstant) expr);
        } else if (expr instanceof OceanBaseColumnReference) {
            visit((OceanBaseColumnReference) expr);
        } else if (expr instanceof OceanBaseUnaryPostfixOperation) {
            visit((OceanBaseUnaryPostfixOperation) expr);
        } else if (expr instanceof OceanBaseComputableFunction) {
            visit((OceanBaseComputableFunction) expr);
        } else if (expr instanceof OceanBaseBinaryLogicalOperation) {
            visit((OceanBaseBinaryLogicalOperation) expr);
        } else if (expr instanceof OceanBaseSelect) {
            visit((OceanBaseSelect) expr);
        } else if (expr instanceof OceanBaseBinaryComparisonOperation) {
            visit((OceanBaseBinaryComparisonOperation) expr);
        } else if (expr instanceof OceanBaseCastOperation) {
            visit((OceanBaseCastOperation) expr);
        } else if (expr instanceof OceanBaseInOperation) {
            visit((OceanBaseInOperation) expr);
        } else if (expr instanceof OceanBaseOrderByTerm) {
            visit((OceanBaseOrderByTerm) expr);
        } else if (expr instanceof OceanBaseExists) {
            visit((OceanBaseExists) expr);
        } else if (expr instanceof OceanBaseStringExpression) {
            visit((OceanBaseStringExpression) expr);
        } else if (expr instanceof OceanBaseTableReference) {
            visit((OceanBaseTableReference) expr);
        } else if (expr instanceof OceanBaseAggregate) {
            visit((OceanBaseAggregate) expr);
        } else if (expr instanceof OceanBaseColumnName) {
            visit((OceanBaseColumnName) expr);
        } else if (expr instanceof OceanBaseText) {
            visit((OceanBaseText) expr);
        } else if (expr instanceof OceanBaseUnaryPrefixOperation) {
            visit((OceanBaseUnaryPrefixOperation) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(OceanBaseExpression expr) {
        OceanBaseToStringVisitor visitor = new OceanBaseToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(OceanBaseExpression expr) {
        OceanBaseExpectedValueVisitor visitor = new OceanBaseExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
