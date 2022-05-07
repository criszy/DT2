package DT2.h2;

import DT2.Randomly;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.h2.H2Provider.H2GlobalState;

public final class H2ViewGenerator {

    private H2ViewGenerator() {
    }

    public static SQLQueryAdapter getQuery(H2GlobalState globalState) {
        int nrColumns = Randomly.smallNumber() + 1;
        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("VIEW ");
        sb.append(globalState.getSchema().getFreeViewName());
        sb.append("(");
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("c");
            sb.append(i);
        }
        sb.append(") AS ");
        sb.append(H2ToStringVisitor.asString(H2RandomQuerySynthesizer.generateSelect(globalState, nrColumns)));
        ExpectedErrors errors = new ExpectedErrors();
        H2Errors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
