package DT2.citus.gen;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.gen.PostgresUpdateGenerator;

public final class CitusUpdateGenerator {

    private CitusUpdateGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter updateQuery = PostgresUpdateGenerator.create(globalState);
        ExpectedErrors errors = updateQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return updateQuery;
    }

}
