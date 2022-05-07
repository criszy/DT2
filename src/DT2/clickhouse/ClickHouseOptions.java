package DT2.clickhouse;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import DT2.DBMSSpecificOptions;
import DT2.OracleFactory;
import DT2.clickhouse.ClickHouseOptions.ClickHouseOracleFactory;
import DT2.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import DT2.clickhouse.oracle.tlp.ClickHouseTLPAggregateOracle;
import DT2.clickhouse.oracle.tlp.ClickHouseTLPDistinctOracle;
import DT2.clickhouse.oracle.tlp.ClickHouseTLPGroupByOracle;
import DT2.clickhouse.oracle.tlp.ClickHouseTLPHavingOracle;
import DT2.clickhouse.oracle.tlp.ClickHouseTLPWhereOracle;
import DT2.common.oracle.TestOracle;

@Parameters(separators = "=", commandDescription = "ClickHouse (default port: " + ClickHouseOptions.DEFAULT_PORT
        + ", default host: " + ClickHouseOptions.DEFAULT_HOST + ")")
public class ClickHouseOptions implements DBMSSpecificOptions<ClickHouseOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8123;

    @Parameter(names = "--oracle")
    public List<ClickHouseOracleFactory> oracle = Arrays.asList(ClickHouseOracleFactory.TLPWhere);

    @Parameter(names = { "--test-joins" }, description = "Allow the generation of JOIN clauses", arity = 1)
    public boolean testJoins = true;

    public enum ClickHouseOracleFactory implements OracleFactory<ClickHouseGlobalState> {
        TLPWhere {
            @Override
            public TestOracle create(ClickHouseGlobalState globalState) throws SQLException {
                return new ClickHouseTLPWhereOracle(globalState);
            }
        },
        TLPDistinct {
            @Override
            public TestOracle create(ClickHouseGlobalState globalState) throws SQLException {
                return new ClickHouseTLPDistinctOracle(globalState);
            }
        },
        TLPGroupBy {
            @Override
            public TestOracle create(ClickHouseGlobalState globalState) throws SQLException {
                return new ClickHouseTLPGroupByOracle(globalState);
            }
        },
        TLPAggregate {
            @Override
            public TestOracle create(ClickHouseGlobalState globalState) throws SQLException {
                return new ClickHouseTLPAggregateOracle(globalState);
            }
        },
        TLPHaving {
            @Override
            public TestOracle create(ClickHouseGlobalState globalState) throws SQLException {
                return new ClickHouseTLPHavingOracle(globalState);
            }
        };

    }

    @Override
    public List<ClickHouseOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
