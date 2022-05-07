
package DT2.mysql;

import java.sql.SQLException;

import DT2.SQLGlobalState;
import DT2.mysql.MySQLOptions.MySQLOracleFactory;

public class MySQLGlobalState extends SQLGlobalState<MySQLOptions, MySQLSchema> {

    @Override
    protected MySQLSchema readSchema() throws SQLException {
        return MySQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOracleFactory.PQS);
    }

}
