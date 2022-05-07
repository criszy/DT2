package DT2.mysql.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.query.SQLQueryAdapter;
import DT2.mysql.MySQLGlobalState;
import DT2.mysql.MySQLSchema.MySQLTable;

/**
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/optimize-table.html">OPTIMIZE TABLE Statement</a>
 */
public class MySQLOptimize {

    private final List<MySQLTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public MySQLOptimize(List<MySQLTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter optimize(MySQLGlobalState globalState) {
        return new MySQLOptimize(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).optimize();
    }

    // OPTIMIZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    private SQLQueryAdapter optimize() {
        sb.append("OPTIMIZE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
