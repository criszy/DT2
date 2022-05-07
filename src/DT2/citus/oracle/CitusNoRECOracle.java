package DT2.citus.oracle;

import DT2.citus.gen.CitusCommon;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.oracle.PostgresNoRECOracle;

public class CitusNoRECOracle extends PostgresNoRECOracle {

    public CitusNoRECOracle(PostgresGlobalState globalState) {
        super(globalState);
        CitusCommon.addCitusErrors(errors);
    }

}
