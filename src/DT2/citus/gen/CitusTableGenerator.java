package DT2.citus.gen;

import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.PostgresSchema;
import DT2.postgres.gen.PostgresTableGenerator;

public class CitusTableGenerator extends PostgresTableGenerator {

    public CitusTableGenerator(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
        super(tableName, newSchema, generateOnlyKnown, globalState);
        CitusCommon.addCitusErrors(errors);
        errors.add("columnar_parallelscan_estimate not implemented"); // see
                                                                      // https://github.com/sqlancer/sqlancer/issues/402
    }

    public static SQLQueryAdapter generate(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
        return new CitusTableGenerator(tableName, newSchema, generateOnlyKnown, globalState).generate();
    }

}
