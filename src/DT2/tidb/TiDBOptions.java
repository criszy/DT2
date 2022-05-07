package DT2.tidb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import DT2.DBMSSpecificOptions;
import DT2.OracleFactory;
import DT2.common.oracle.CompositeTestOracle;
import DT2.common.oracle.TestOracle;
import DT2.tidb.TiDBOptions.TiDBOracleFactory;
import DT2.tidb.TiDBProvider.TiDBGlobalState;
import DT2.tidb.oracle.TiDBTLPHavingOracle;
import DT2.tidb.oracle.TiDBTLPWhereOracle;
import DT2.tidb.oracle.TiDBTxnDiffOracle;

@Parameters(separators = "=", commandDescription = "TiDB (default port: " + TiDBOptions.DEFAULT_PORT
        + ", default host: " + TiDBOptions.DEFAULT_HOST + ")")
public class TiDBOptions implements DBMSSpecificOptions<TiDBOracleFactory> {
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 4000;

    @Parameter(names = "--oracle")
    public List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.QUERY_PARTITIONING);

    public enum TiDBOracleFactory implements OracleFactory<TiDBGlobalState> {
        DIFF {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTxnDiffOracle(globalState);
            }
        },
        HAVING {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTLPHavingOracle(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTLPWhereOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new TiDBTLPWhereOracle(globalState));
                oracles.add(new TiDBTLPHavingOracle(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };

    }

    @Override
    public List<TiDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
