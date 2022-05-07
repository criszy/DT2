package DT2.common.oracle.txndiff;

import java.util.ArrayList;

public class ReproduceState {

    static public final ArrayList<String> statements = new ArrayList<>();

    public static void logStatement(String query) {
        if (query == null) {
            throw new IllegalArgumentException();
        }
        statements.add(query);
    }

    public static ArrayList<String> getStatements() {
        return statements;
    }
}
