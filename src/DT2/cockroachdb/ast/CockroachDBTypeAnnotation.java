package DT2.cockroachdb.ast;

import DT2.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import DT2.common.ast.UnaryNode;

public class CockroachDBTypeAnnotation extends UnaryNode<CockroachDBExpression> implements CockroachDBExpression {

    private final CockroachDBCompositeDataType type;

    public CockroachDBTypeAnnotation(CockroachDBExpression expr, CockroachDBCompositeDataType type) {
        super(expr);
        this.type = type;
    }

    @Override
    public String getOperatorRepresentation() {
        return ":::" + type.toString();
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
