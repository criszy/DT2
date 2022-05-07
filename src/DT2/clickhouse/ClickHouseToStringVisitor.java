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
import DT2.common.visitor.ToStringVisitor;

public class ClickHouseToStringVisitor extends ToStringVisitor<ClickHouseExpression> implements ClickHouseVisitor {

    @Override
    public void visitSpecific(ClickHouseExpression expr) {
        ClickHouseVisitor.super.visit(expr);
    }

    @Override
    public void visit(ClickHouseBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOperator().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseBinaryLogicalOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseUnaryPrefixOperation op) {
        sb.append(op.getOperatorRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseUnaryPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorRepresentation());
    }

    @Override
    public void visit(ClickHouseConstant constant) {
        sb.append(constant.toString());
    }

    @Override
    public void visit(ClickHouseSelect select, boolean inner) {
        if (inner) {
            sb.append("(");
        }
        sb.append("SELECT ");
        switch (select.getFromOptions()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            break;
        case ALL:
            sb.append("");
            break;
        default:
            throw new AssertionError(select.getFromOptions());
        }

        visit(select.getFetchColumns());
        sb.append(" FROM ");
        visit(select.getFromList());
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByClause().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByClause());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByClause().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByClause());
        }
        if (inner) {
            sb.append(")");
        }
    }

    @Override
    public void visit(ClickHouseTableReference tableReference) {
        sb.append(tableReference.getTable().getName());
    }

    @Override
    public void visit(ClickHouseAggregate aggregate) {
        sb.append(aggregate.getFunc());
        sb.append("(");
        visit(aggregate.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseCastOperation cast) {
        sb.append("CAST(");
        visit(cast.getExpression());
        sb.append(" AS ");
        sb.append(cast.getType().toString());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseExpression.ClickHouseJoin join) {

    }

    @Override
    public void visit(ClickHouseColumnReference c) {
        if (c.getColumn().getTable() == null) {
            sb.append(c.getColumn().getName());
        } else {
            sb.append(c.getColumn().getFullQualifiedName());
        }
    }

    public static String asString(ClickHouseExpression expr) {
        ClickHouseToStringVisitor visitor = new ClickHouseToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
