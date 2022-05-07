package DT2.citus.gen;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.gen.PostgresInsertGenerator;

public final class CitusInsertGenerator {

    private CitusInsertGenerator() {
    }

    public static SQLQueryAdapter insert(PostgresGlobalState globalState) {
        SQLQueryAdapter insertQuery = PostgresInsertGenerator.insert(globalState);
        ExpectedErrors errors = insertQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return insertQuery;
    }

}
