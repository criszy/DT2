package DT2.citus.gen;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.gen.PostgresDeleteGenerator;

public final class CitusDeleteGenerator {

    private CitusDeleteGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter deleteQuery = PostgresDeleteGenerator.create(globalState);
        ExpectedErrors errors = deleteQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return deleteQuery;
    }

}
