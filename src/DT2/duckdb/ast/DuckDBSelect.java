package DT2.duckdb.ast;

import DT2.common.ast.SelectBase;
import DT2.common.ast.newast.Node;

public class DuckDBSelect extends SelectBase<Node<DuckDBExpression>> implements Node<DuckDBExpression> {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

}
