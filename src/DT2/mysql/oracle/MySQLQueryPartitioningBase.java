package DT2.mysql.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import DT2.common.gen.ExpressionGenerator;
import DT2.common.oracle.TernaryLogicPartitioningOracleBase;
import DT2.common.oracle.TestOracle;
import DT2.mysql.MySQLErrors;
import DT2.mysql.MySQLGlobalState;
import DT2.mysql.MySQLSchema;
import DT2.mysql.MySQLSchema.MySQLTable;
import DT2.mysql.MySQLSchema.MySQLTables;
import DT2.mysql.ast.MySQLColumnReference;
import DT2.mysql.ast.MySQLExpression;
import DT2.mysql.ast.MySQLSelect;
import DT2.mysql.ast.MySQLTableReference;
import DT2.mysql.gen.MySQLExpressionGenerator;

public abstract class MySQLQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<MySQLExpression, MySQLGlobalState> implements TestOracle {

    MySQLSchema s;
    MySQLTables targetTables;
    MySQLExpressionGenerator gen;
    MySQLSelect select;

    public MySQLQueryPartitioningBase(MySQLGlobalState state) {
        super(state);
        MySQLErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new MySQLExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new MySQLSelect();
        select.setFetchColumns(generateFetchColumns());
        List<MySQLTable> tables = targetTables.getTables();
        List<MySQLExpression> tableList = tables.stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        // List<MySQLExpression> joins = MySQLJoin.getJoins(tableList, state);
        select.setFromList(tableList);
        select.setWhereClause(null);
        // select.setJoins(joins);
    }

    List<MySQLExpression> generateFetchColumns() {
        return Arrays.asList(MySQLColumnReference.create(targetTables.getColumns().get(0), null));
    }

    @Override
    protected ExpressionGenerator<MySQLExpression> getGen() {
        return gen;
    }

}
