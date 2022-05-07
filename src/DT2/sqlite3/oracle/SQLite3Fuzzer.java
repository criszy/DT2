package DT2.sqlite3.oracle;

import DT2.Randomly;
import DT2.common.oracle.TestOracle;
import DT2.common.query.SQLQueryAdapter;
import DT2.sqlite3.SQLite3GlobalState;
import DT2.sqlite3.SQLite3Visitor;

// tries to trigger a crash
public class SQLite3Fuzzer implements TestOracle {

    private final SQLite3GlobalState globalState;

    public SQLite3Fuzzer(SQLite3GlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        String s = SQLite3Visitor
                .asString(SQLite3RandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1)) + ";";
        try {
            if (globalState.getDbmsSpecificOptions().executeQuery) {
                globalState.executeStatement(new SQLQueryAdapter(s));
                globalState.getManager().incrementSelectQueryCount();
            }
        } catch (Error e) {

        }
    }

}
