package sqlancer.common.oracle.txndiff;

import java.sql.ResultSet;

public interface ResultSetHandler {
    void handle(ResultSet rs);
}
