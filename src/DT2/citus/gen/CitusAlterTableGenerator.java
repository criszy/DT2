package DT2.citus.gen;

import java.util.List;

import DT2.IgnoreMeException;
import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.postgres.PostgresGlobalState;
import DT2.postgres.PostgresSchema.PostgresTable;
import DT2.postgres.gen.PostgresAlterTableGenerator;

public class CitusAlterTableGenerator extends PostgresAlterTableGenerator {

    public CitusAlterTableGenerator(PostgresTable randomTable, PostgresGlobalState globalState,
            boolean generateOnlyKnown) {
        super(randomTable, globalState, generateOnlyKnown);
    }

    public static SQLQueryAdapter create(PostgresTable randomTable, PostgresGlobalState globalState,
            boolean generateOnlyKnown) {
        return new CitusAlterTableGenerator(randomTable, globalState, generateOnlyKnown).generate();
    }

    @Override
    public List<Action> getActions(ExpectedErrors errors) {
        List<Action> action = super.getActions(errors);
        CitusCommon.addCitusErrors(errors);
        action.remove(Action.ALTER_COLUMN_SET_STATISTICS);
        action.remove(Action.ALTER_COLUMN_SET_ATTRIBUTE_OPTION);
        action.remove(Action.ALTER_COLUMN_RESET_ATTRIBUTE_OPTION);
        action.remove(Action.ALTER_COLUMN_SET_STORAGE);
        action.remove(Action.DISABLE_ROW_LEVEL_SECURITY);
        action.remove(Action.ENABLE_ROW_LEVEL_SECURITY);
        action.remove(Action.FORCE_ROW_LEVEL_SECURITY);
        action.remove(Action.NO_FORCE_ROW_LEVEL_SECURITY);
        action.remove(Action.CLUSTER_ON);
        action.remove(Action.SET_WITHOUT_CLUSTER);
        action.remove(Action.SET_WITH_OIDS);
        action.remove(Action.SET_WITHOUT_OIDS);
        action.remove(Action.SET_LOGGED_UNLOGGED);
        action.remove(Action.NOT_OF);
        action.remove(Action.OWNER_TO);
        action.remove(Action.REPLICA_IDENTITY);
        if (action.isEmpty()) {
            throw new IgnoreMeException();
        }
        return action;
    }

}
