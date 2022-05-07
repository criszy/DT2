package DT2.common.schema;

import java.util.List;

import DT2.IgnoreMeException;
import DT2.SQLGlobalState;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.query.SQLancerResultSet;

public class AbstractRelationalTable<C extends AbstractTableColumn<?, ?>, I extends TableIndex, G extends SQLGlobalState<?, ?>>
        extends AbstractTable<C, I, G> {

    public AbstractRelationalTable(String name, List<C> columns, List<I> indexes, boolean isView) {
        super(name, columns, indexes, isView);
    }

    @Override
    public long getNrRows(G globalState) {
        if (rowCount == NO_ROW_COUNT_AVAILABLE) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT COUNT(*) FROM " + name);
            try (SQLancerResultSet query = q.executeAndGet(globalState)) {
                if (query == null) {
                    throw new IgnoreMeException();
                }
                query.next();
                rowCount = query.getLong(1);
                return rowCount;
            } catch (Throwable t) {
                // an exception might be expected, for example, when invalid view is created
                throw new IgnoreMeException();
            }
        } else {
            return rowCount;
        }
    }

}
