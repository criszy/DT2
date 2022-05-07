package DT2.oceanbase;

import DT2.IgnoreMeException;
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

public class OceanBaseExpectedValueVisitor implements OceanBaseVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(OceanBaseExpression expr) {
        OceanBaseToStringVisitor v = new OceanBaseToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < nrTabs; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(OceanBaseExpression expr) {
        nrTabs++;
        try {
            OceanBaseVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    @Override
    public void visit(OceanBaseConstant constant) {
        print(constant);
    }

    @Override
    public void visit(OceanBaseColumnReference column) {
        print(column);
    }

    @Override
    public void visit(OceanBaseUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(OceanBaseComputableFunction f) {
        print(f);
        for (OceanBaseExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(OceanBaseBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(OceanBaseSelect select) {
        for (OceanBaseExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(OceanBaseBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(OceanBaseCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(OceanBaseInOperation op) {
        print(op);
        visit(op.getExpr());
        for (OceanBaseExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(OceanBaseOrderByTerm op) {
    }

    @Override
    public void visit(OceanBaseExists op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(OceanBaseStringExpression op) {
        print(op);
    }

    @Override
    public void visit(OceanBaseTableReference ref) {
    }

    @Override
    public void visit(OceanBaseAggregate aggr) {
    }

    @Override
    public void visit(OceanBaseColumnName aggr) {
    }

    @Override
    public void visit(OceanBaseText func) {
    }

    @Override
    public void visit(OceanBaseUnaryPrefixOperation op) {
        print(op);
        visit(op.getExpr());
    }

}
