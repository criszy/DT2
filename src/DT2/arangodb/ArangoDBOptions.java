package DT2.arangodb;

import static DT2.arangodb.ArangoDBOptions.ArangoDBOracleFactory.QUERY_PARTITIONING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import DT2.DBMSSpecificOptions;
import DT2.OracleFactory;
import DT2.arangodb.ArangoDBProvider.ArangoDBGlobalState;
import DT2.arangodb.test.ArangoDBQueryPartitioningWhereTester;
import DT2.common.oracle.CompositeTestOracle;
import DT2.common.oracle.TestOracle;

@Parameters(commandDescription = "ArangoDB (experimental)")
public class ArangoDBOptions implements DBMSSpecificOptions<ArangoDBOptions.ArangoDBOracleFactory> {

    @Parameter(names = "--oracle")
    public List<ArangoDBOracleFactory> oracles = Arrays.asList(QUERY_PARTITIONING);

    @Parameter(names = "--test-random-type-inserts", description = "Insert random types instead of schema types.")
    public boolean testRandomTypeInserts;

    @Parameter(names = "--max-number-indexes", description = "The maximum number of indexes used.", arity = 1)
    public int maxNumberIndexes = 15;

    @Parameter(names = "--with-optimizer-rule-tests", description = "Adds an additional query, where a random set"
            + "of optimizer rules are disabled.", arity = 1)
    public boolean withOptimizerRuleTests;

    @Override
    public List<ArangoDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

    public enum ArangoDBOracleFactory implements OracleFactory<ArangoDBGlobalState> {
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(ArangoDBGlobalState globalState) throws Exception {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new ArangoDBQueryPartitioningWhereTester(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        }
    }
}
