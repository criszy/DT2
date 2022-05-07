package DT2.citus.gen;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.gen.PostgresInsertGenerator;

public final class CitusIndexGenerator {

    private CitusIndexGenerator() {
    }

    public static SQLQueryAdapter generate(PostgresGlobalState globalState) {
        SQLQueryAdapter createIndexQuery = PostgresInsertGenerator.insert(globalState);
        ExpectedErrors errors = createIndexQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return createIndexQuery;
    }

}
