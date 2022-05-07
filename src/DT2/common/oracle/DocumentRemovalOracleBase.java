package DT2.common.oracle;

import DT2.GlobalState;
import DT2.common.gen.ExpressionGenerator;

public abstract class DocumentRemovalOracleBase<E, S extends GlobalState<?, ?, ?>> implements TestOracle {

    protected E predicate;

    protected final S state;

    protected DocumentRemovalOracleBase(S state) {
        this.state = state;
    }

    protected void initializeDocumentRemovalOracle() {
        ExpressionGenerator<E> gen = getGen();
        if (gen == null) {
            throw new IllegalStateException();
        }
        predicate = gen.generatePredicate();
        if (predicate == null) {
            throw new IllegalStateException();
        }
    }

    protected abstract ExpressionGenerator<E> getGen();

}
