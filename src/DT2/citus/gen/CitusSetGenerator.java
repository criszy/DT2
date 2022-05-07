package DT2.citus.gen;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.gen.PostgresSetGenerator;

public final class CitusSetGenerator {

    private CitusSetGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter setQuery = PostgresSetGenerator.create(globalState);
        ExpectedErrors errors = setQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return setQuery;
    }

}
