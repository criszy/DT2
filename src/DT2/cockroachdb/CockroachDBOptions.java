package DT2.cockroachdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import DT2.DBMSSpecificOptions;
import DT2.OracleFactory;
import DT2.cockroachdb.CockroachDBOptions.CockroachDBOracleFactory;
import DT2.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import DT2.cockroachdb.oracle.CockroachDBNoRECOracle;
import DT2.cockroachdb.oracle.tlp.CockroachDBTLPAggregateOracle;
import DT2.cockroachdb.oracle.tlp.CockroachDBTLPDistinctOracle;
import DT2.cockroachdb.oracle.tlp.CockroachDBTLPExtendedWhereOracle;
import DT2.cockroachdb.oracle.tlp.CockroachDBTLPGroupByOracle;
import DT2.cockroachdb.oracle.tlp.CockroachDBTLPHavingOracle;
import DT2.cockroachdb.oracle.tlp.CockroachDBTLPWhereOracle;
import DT2.common.oracle.CompositeTestOracle;
import DT2.common.oracle.TestOracle;

@Parameters(separators = "=", commandDescription = "CockroachDB (default port: " + CockroachDBOptions.DEFAULT_PORT
        + " default host: " + CockroachDBOptions.DEFAULT_HOST + ")")
public class CockroachDBOptions implements DBMSSpecificOptions<CockroachDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 26257;

    @Parameter(names = "--oracle")
    public CockroachDBOracleFactory oracle = CockroachDBOracleFactory.NOREC;

    public enum CockroachDBOracleFactory implements OracleFactory<CockroachDBGlobalState> {
        NOREC {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBNoRECOracle(globalState);
            }
        },
        AGGREGATE {

            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPAggregateOracle(globalState);
            }

        },
        GROUP_BY {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPGroupByOracle(globalState);
            }
        },
        HAVING {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPHavingOracle(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPWhereOracle(globalState);
            }
        },
        DISTINCT {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPDistinctOracle(globalState);
            }
        },
        EXTENDED_WHERE {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPExtendedWhereOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new CockroachDBTLPAggregateOracle(globalState));
                oracles.add(new CockroachDBTLPHavingOracle(globalState));
                oracles.add(new CockroachDBTLPWhereOracle(globalState));
                oracles.add(new CockroachDBTLPGroupByOracle(globalState));
                oracles.add(new CockroachDBTLPExtendedWhereOracle(globalState));
                oracles.add(new CockroachDBTLPDistinctOracle(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };

    }

    @Parameter(names = {
            "--test-hash-indexes" }, description = "Test the USING HASH WITH BUCKET_COUNT=n_buckets option in CREATE INDEX")
    public boolean testHashIndexes = true;

    @Parameter(names = { "--test-temp-tables" }, description = "Test TEMPORARY tables")
    public boolean testTempTables = true;

    @Parameter(names = {
            "--increased-vectorization" }, description = "Generate VECTORIZE=on with a higher probability (which found a number of bugs in the past)")
    public boolean makeVectorizationMoreLikely = true;

    @Override
    public List<CockroachDBOracleFactory> getTestOracleFactory() {
        return Arrays.asList(oracle);
    }

}
