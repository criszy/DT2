package DT2.tidb.ast;

import DT2.common.ast.SelectBase;

public class TiDBSelect extends SelectBase<TiDBExpression> implements TiDBExpression {

    private TiDBExpression hint;

    public void setHint(TiDBExpression hint) {
        this.hint = hint;
    }

    public TiDBExpression getHint() {
        return hint;
    }

}
