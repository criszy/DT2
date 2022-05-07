package DT2.cockroachdb.ast;

import DT2.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import DT2.common.visitor.UnaryOperation;

public class CockroachDBCast implements UnaryOperation<CockroachDBExpression>, CockroachDBExpression {

    private final CockroachDBExpression expr;
    private final CockroachDBCompositeDataType type;

    public CockroachDBCast(CockroachDBExpression expr, CockroachDBCompositeDataType type) {
        this.expr = expr;
        this.type = type;
    }

    @Override
    public CockroachDBExpression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return "::" + type.toString();
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
