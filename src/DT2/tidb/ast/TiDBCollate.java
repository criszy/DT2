package DT2.tidb.ast;

import DT2.common.ast.UnaryNode;

public class TiDBCollate extends UnaryNode<TiDBExpression> implements TiDBExpression {

    private final String collate;

    public TiDBCollate(TiDBExpression expr, String text) {
        super(expr);
        this.collate = text;
    }

    @Override
    public String getOperatorRepresentation() {
        return String.format("COLLATE '%s'", collate);
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
