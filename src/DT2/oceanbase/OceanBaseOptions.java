package DT2.oceanbase;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import DT2.DBMSSpecificOptions;
import DT2.OracleFactory;
import DT2.common.oracle.TestOracle;
import DT2.oceanbase.OceanBaseOptions.OceanBaseOracleFactory;
import DT2.oceanbase.oracle.OceanBaseNoRECOracle;
import DT2.oceanbase.oracle.OceanBasePivotedQuerySynthesisOracle;
import DT2.oceanbase.oracle.OceanBaseTLPWhereOracle;

@Parameters(separators = "=", commandDescription = "OceanBase (default port: " + OceanBaseOptions.DEFAULT_PORT
        + ", default host: " + OceanBaseOptions.DEFAULT_HOST + ")")
public class OceanBaseOptions implements DBMSSpecificOptions<OceanBaseOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 2881;

    @Parameter(names = "--oracle")
    public List<OceanBaseOracleFactory> oracles = Arrays.asList(OceanBaseOracleFactory.TLP_WHERE);

    public enum OceanBaseOracleFactory implements OracleFactory<OceanBaseGlobalState> {

        TLP_WHERE {
            @Override
            public TestOracle create(OceanBaseGlobalState globalState) throws SQLException {
                return new OceanBaseTLPWhereOracle(globalState);
            }
        },
        NoREC {
            @Override
            public TestOracle create(OceanBaseGlobalState globalState) throws SQLException {
                return new OceanBaseNoRECOracle(globalState);
            }
        },
        PQS {

            @Override
            public TestOracle create(OceanBaseGlobalState globalState) throws SQLException {
                return new OceanBasePivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        }
    }

    @Parameter(names = { "--query-timeout" }, description = "Query timeout")
    public int queryTimeout = 1000000000;
    @Parameter(names = { "--transaction-timeout" }, description = "Transaction timeout")
    public int trxTimeout = 1000000000;

    @Override
    public List<OceanBaseOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
