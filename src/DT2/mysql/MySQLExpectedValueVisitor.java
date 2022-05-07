package DT2.mysql;

import DT2.IgnoreMeException;
import DT2.mysql.ast.MySQLBetweenOperation;
import DT2.mysql.ast.MySQLBinaryComparisonOperation;
import DT2.mysql.ast.MySQLBinaryLogicalOperation;
import DT2.mysql.ast.MySQLBinaryOperation;
import DT2.mysql.ast.MySQLCastOperation;
import DT2.mysql.ast.MySQLCollate;
import DT2.mysql.ast.MySQLColumnReference;
import DT2.mysql.ast.MySQLComputableFunction;
import DT2.mysql.ast.MySQLConstant;
import DT2.mysql.ast.MySQLExists;
import DT2.mysql.ast.MySQLExpression;
import DT2.mysql.ast.MySQLInOperation;
import DT2.mysql.ast.MySQLOrderByTerm;
import DT2.mysql.ast.MySQLSelect;
import DT2.mysql.ast.MySQLStringExpression;
import DT2.mysql.ast.MySQLTableReference;
import DT2.mysql.ast.MySQLUnaryPostfixOperation;

public class MySQLExpectedValueVisitor implements MySQLVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(MySQLExpression expr) {
        MySQLToStringVisitor v = new MySQLToStringVisitor();
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
    public void visit(MySQLExpression expr) {
        nrTabs++;
        try {
            MySQLVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    @Override
    public void visit(MySQLConstant constant) {
        print(constant);
    }

    @Override
    public void visit(MySQLColumnReference column) {
        print(column);
    }

    @Override
    public void visit(MySQLUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(MySQLComputableFunction f) {
        print(f);
        for (MySQLExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(MySQLBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(MySQLSelect select) {
        for (MySQLExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(MySQLBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(MySQLCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(MySQLInOperation op) {
        print(op);
        for (MySQLExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(MySQLBinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(MySQLOrderByTerm op) {
    }

    @Override
    public void visit(MySQLExists op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(MySQLStringExpression op) {
        print(op);
    }

    @Override
    public void visit(MySQLBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(MySQLTableReference ref) {
    }

    @Override
    public void visit(MySQLCollate collate) {
        print(collate);
        visit(collate.getExpectedValue());
    }

}
