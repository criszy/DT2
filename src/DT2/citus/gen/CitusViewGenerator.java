package DT2.citus.gen;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.gen.PostgresViewGenerator;

public final class CitusViewGenerator {

    private CitusViewGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter viewQuery = PostgresViewGenerator.create(globalState);
        ExpectedErrors errors = viewQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return viewQuery;
    }

}
