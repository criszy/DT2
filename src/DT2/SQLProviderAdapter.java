package DT2;

import java.util.List;

import DT2.common.log.LoggableFactory;
import DT2.common.log.SQLLoggableFactory;
import DT2.common.query.SQLQueryAdapter;
import DT2.common.schema.AbstractSchema;
import DT2.common.schema.AbstractTable;

public abstract class SQLProviderAdapter<G extends SQLGlobalState<O, ? extends AbstractSchema<G, ?>>, O extends DBMSSpecificOptions<? extends OracleFactory<G>>>
        extends ProviderAdapter<G, O, SQLConnection> {
    public SQLProviderAdapter(Class<G> globalClass, Class<O> optionClass) {
        super(globalClass, optionClass);
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new SQLLoggableFactory();
    }

    @Override
    protected void checkViewsAreValid(G globalState) {
        List<? extends AbstractTable<?, ?, ?>> views = globalState.getSchema().getViews();
        for (AbstractTable<?, ?, ?> view : views) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT 1 FROM " + view.getName() + " LIMIT 1");
            try {
                q.execute(globalState);
            } catch (Throwable t) {
                throw new IgnoreMeException();
            }
        }
    }
}
