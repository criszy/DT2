package DT2.cockroachdb.ast;

import DT2.common.ast.SelectBase;

public class CockroachDBSelect extends SelectBase<CockroachDBExpression> implements CockroachDBExpression {

    private boolean isDistinct;

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

}
