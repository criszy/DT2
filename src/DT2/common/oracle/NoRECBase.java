package DT2.common.oracle;

import DT2.Main.StateLogger;
import DT2.MainOptions;
import DT2.SQLConnection;
import DT2.SQLGlobalState;
import DT2.common.query.ExpectedErrors;

public abstract class NoRECBase<S extends SQLGlobalState<?, ?>> implements TestOracle {

    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;
    protected String optimizedQueryString;
    protected String unoptimizedQueryString;

    public NoRECBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
    }

}
