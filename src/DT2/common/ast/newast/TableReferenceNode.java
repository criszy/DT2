package DT2.common.ast.newast;

import DT2.common.schema.AbstractTable;

public class TableReferenceNode<E, T extends AbstractTable<?, ?, ?>> implements Node<E> {

    private final T t;

    public TableReferenceNode(T table) {
        this.t = table;
    }

    public T getTable() {
        return t;
    }

}
