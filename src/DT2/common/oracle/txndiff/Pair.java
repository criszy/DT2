package DT2.common.oracle.txndiff;

import lombok.Data;

@Data
public class Pair<E extends Object, F extends Object> {
    private E left;
    private F right;

    public Pair(E left, F right) {
        this.left = left;
        this.right = right;
    }

    public E getLeft() { return left; }

    public F getRight() { return right; }

    public boolean equals(Pair<E, F> that) {
        if (that == null) {
            return false;
        }
        if (this == that) {
            return true;
        }
        if (left.equals(that.left) && right.equals(that.right)) {
            return true;
        }
        return false;
    }
}
