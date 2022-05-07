package DT2.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.ast.newast.ColumnReferenceNode;
import DT2.common.ast.newast.Node;
import DT2.common.ast.newast.TableReferenceNode;
import DT2.common.gen.ExpressionGenerator;
import DT2.common.oracle.TernaryLogicPartitioningOracleBase;
import DT2.common.oracle.TestOracle;
import DT2.duckdb.DuckDBErrors;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema;
import DT2.duckdb.DuckDBSchema.DuckDBColumn;
import DT2.duckdb.DuckDBSchema.DuckDBTable;
import DT2.duckdb.DuckDBSchema.DuckDBTables;
import DT2.duckdb.ast.DuckDBExpression;
import DT2.duckdb.ast.DuckDBJoin;
import DT2.duckdb.ast.DuckDBSelect;
import DT2.duckdb.gen.DuckDBExpressionGenerator;

public class DuckDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<DuckDBExpression>, DuckDBGlobalState> implements TestOracle {

    DuckDBSchema s;
    DuckDBTables targetTables;
    DuckDBExpressionGenerator gen;
    DuckDBSelect select;

    public DuckDBQueryPartitioningBase(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new DuckDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new DuckDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<DuckDBTable> tables = targetTables.getTables();
        List<TableReferenceNode<DuckDBExpression, DuckDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DuckDBExpression, DuckDBTable>(t)).collect(Collectors.toList());
        List<Node<DuckDBExpression>> joins = DuckDBJoin.getJoins(tableList, state);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<Node<DuckDBExpression>> generateFetchColumns() {
        List<Node<DuckDBExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new DuckDBColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(c)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<DuckDBExpression>> getGen() {
        return gen;
    }

}
