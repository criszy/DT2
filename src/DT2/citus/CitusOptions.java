package DT2.citus;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import DT2.OracleFactory;
import DT2.citus.oracle.CitusNoRECOracle;
import DT2.citus.oracle.tlp.CitusTLPAggregateOracle;
import DT2.citus.oracle.tlp.CitusTLPHavingOracle;
import DT2.citus.oracle.tlp.CitusTLPWhereOracle;
import DT2.common.oracle.CompositeTestOracle;
import DT2.common.oracle.TestOracle;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.PostgresOptions;
import DT2.postgres.oracle.PostgresPivotedQuerySynthesisOracle;

public class CitusOptions extends PostgresOptions {

    @Parameter(names = "--repartition", description = "Specifies whether repartition joins should be allowed", arity = 1)
    public boolean repartition = true;

    @Parameter(names = "--citusoracle", description = "Specifies which test oracle should be used for Citus extension to PostgreSQL")
    public List<CitusOracleFactory> citusOracle = Arrays.asList(CitusOracleFactory.QUERY_PARTITIONING);

    public enum CitusOracleFactory implements OracleFactory<PostgresGlobalState> {
        NOREC {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                CitusGlobalState citusGlobalState = (CitusGlobalState) globalState;
                return new CitusNoRECOracle(citusGlobalState);
            }
        },
        PQS {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                return new PostgresPivotedQuerySynthesisOracle(globalState);
            }
        },
        HAVING {

            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                CitusGlobalState citusGlobalState = (CitusGlobalState) globalState;
                return new CitusTLPHavingOracle(citusGlobalState);
            }

        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                CitusGlobalState citusGlobalState = (CitusGlobalState) globalState;
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new CitusTLPWhereOracle(citusGlobalState));
                oracles.add(new CitusTLPHavingOracle(citusGlobalState));
                oracles.add(new CitusTLPAggregateOracle(citusGlobalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };

    }

}
