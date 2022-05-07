package DT2.tidb.visitor;

import DT2.tidb.ast.TiDBAggregate;
import DT2.tidb.ast.TiDBCase;
import DT2.tidb.ast.TiDBCastOperation;
import DT2.tidb.ast.TiDBColumnReference;
import DT2.tidb.ast.TiDBConstant;
import DT2.tidb.ast.TiDBExpression;
import DT2.tidb.ast.TiDBFunctionCall;
import DT2.tidb.ast.TiDBJoin;
import DT2.tidb.ast.TiDBSelect;
import DT2.tidb.ast.TiDBTableReference;
import DT2.tidb.ast.TiDBText;

public interface TiDBVisitor {

    default void visit(TiDBExpression expr) {
        if (expr instanceof TiDBConstant) {
            visit((TiDBConstant) expr);
        } else if (expr instanceof TiDBColumnReference) {
            visit((TiDBColumnReference) expr);
        } else if (expr instanceof TiDBSelect) {
            visit((TiDBSelect) expr);
        } else if (expr instanceof TiDBTableReference) {
            visit((TiDBTableReference) expr);
        } else if (expr instanceof TiDBFunctionCall) {
            visit((TiDBFunctionCall) expr);
        } else if (expr instanceof TiDBJoin) {
            visit((TiDBJoin) expr);
        } else if (expr instanceof TiDBText) {
            visit((TiDBText) expr);
        } else if (expr instanceof TiDBAggregate) {
            visit((TiDBAggregate) expr);
        } else if (expr instanceof TiDBCastOperation) {
            visit((TiDBCastOperation) expr);
        } else if (expr instanceof TiDBCase) {
            visit((TiDBCase) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    void visit(TiDBCase caseExpr);

    void visit(TiDBCastOperation cast);

    void visit(TiDBAggregate aggr);

    void visit(TiDBFunctionCall call);

    void visit(TiDBConstant expr);

    void visit(TiDBColumnReference expr);

    void visit(TiDBTableReference expr);

    void visit(TiDBSelect select);

    void visit(TiDBJoin join);

    void visit(TiDBText text);

    static String asString(TiDBExpression expr) {
        TiDBToStringVisitor v = new TiDBToStringVisitor();
        v.visit(expr);
        return v.getString();
    }

}
