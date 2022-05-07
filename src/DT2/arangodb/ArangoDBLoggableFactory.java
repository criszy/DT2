package DT2.arangodb;

import java.util.Arrays;

import DT2.common.log.Loggable;
import DT2.common.log.LoggableFactory;
import DT2.common.log.LoggedString;
import DT2.common.query.Query;

public class ArangoDBLoggableFactory extends LoggableFactory {
    @Override
    protected Loggable createLoggable(String input, String suffix) {
        return new LoggedString(input + suffix);
    }

    @Override
    public Query<?> getQueryForStateToReproduce(String queryString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Query<?> commentOutQuery(Query<?> query) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Loggable infoToLoggable(String time, String databaseName, String databaseVersion, long seedValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("// Time: ").append(time).append("\n");
        sb.append("// Database: ").append(databaseName).append("\n");
        sb.append("// Database version: ").append(databaseVersion).append("\n");
        sb.append("// seed value: ").append(seedValue).append("\n");
        return new LoggedString(sb.toString());
    }

    @Override
    public Loggable convertStacktraceToLoggable(Throwable throwable) {
        return new LoggedString(Arrays.toString(throwable.getStackTrace()) + "\n" + throwable.getMessage());
    }
}
