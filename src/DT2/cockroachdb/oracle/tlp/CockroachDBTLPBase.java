package DT2.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.cockroachdb.CockroachDBErrors;
import DT2.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import DT2.cockroachdb.CockroachDBSchema;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBTable;
import DT2.cockroachdb.CockroachDBSchema.CockroachDBTables;
import DT2.cockroachdb.ast.CockroachDBColumnReference;
import DT2.cockroachdb.ast.CockroachDBExpression;
import DT2.cockroachdb.ast.CockroachDBSelect;
import DT2.cockroachdb.ast.CockroachDBTableReference;
import DT2.cockroachdb.gen.CockroachDBExpressionGenerator;
import DT2.cockroachdb.oracle.CockroachDBNoRECOracle;
import DT2.common.gen.ExpressionGenerator;
import DT2.common.oracle.TernaryLogicPartitioningOracleBase;
import DT2.common.oracle.TestOracle;

public class CockroachDBTLPBase extends
        TernaryLogicPartitioningOracleBase<CockroachDBExpression, CockroachDBGlobalState> implements TestOracle {

    CockroachDBSchema s;
    CockroachDBTables targetTables;
    CockroachDBExpressionGenerator gen;
    CockroachDBSelect select;

    public CockroachDBTLPBase(CockroachDBGlobalState state) {
        super(state);
        CockroachDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new CockroachDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new CockroachDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<CockroachDBTable> tables = targetTables.getTables();
        List<CockroachDBExpression> tableList = tables.stream().map(t -> new CockroachDBTableReference(t))
                .collect(Collectors.toList());
        List<CockroachDBExpression> joins = CockroachDBNoRECOracle.getJoins(tableList, state);
        select.setJoinList(joins);
        select.setFromList(tableList);
        select.setWhereClause(null);
    }

    List<CockroachDBExpression> generateFetchColumns() {
        List<CockroachDBExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new CockroachDBColumnReference(new CockroachDBColumn("*", null, false, false)));
        } else {
            columns.addAll(Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new CockroachDBColumnReference(c)).collect(Collectors.toList()));
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<CockroachDBExpression> getGen() {
        return gen;
    }

}
