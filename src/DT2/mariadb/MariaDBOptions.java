package DT2.mariadb;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import DT2.DBMSSpecificOptions;
import DT2.OracleFactory;
import DT2.common.oracle.TestOracle;
import DT2.mariadb.MariaDBOptions.MariaDBOracleFactory;
import DT2.mariadb.MariaDBProvider.MariaDBGlobalState;
import DT2.mariadb.oracle.MariaDBNoRECOracle;

@Parameters(separators = "=", commandDescription = "MariaDB (default port: " + MariaDBOptions.DEFAULT_PORT
        + ", default host: " + MariaDBOptions.DEFAULT_HOST + ")")
public class MariaDBOptions implements DBMSSpecificOptions<MariaDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<MariaDBOracleFactory> oracles = Arrays.asList(MariaDBOracleFactory.NOREC);

    public enum MariaDBOracleFactory implements OracleFactory<MariaDBGlobalState> {

        NOREC {

            @Override
            public TestOracle create(MariaDBGlobalState globalState) throws SQLException {
                return new MariaDBNoRECOracle(globalState);
            }

        }
    }

    @Override
    public List<MariaDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
