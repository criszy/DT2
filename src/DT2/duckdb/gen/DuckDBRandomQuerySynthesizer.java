package DT2.duckdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.ast.newast.Node;
import DT2.common.ast.newast.TableReferenceNode;
import DT2.duckdb.DuckDBProvider.DuckDBGlobalState;
import DT2.duckdb.DuckDBSchema.DuckDBTable;
import DT2.duckdb.DuckDBSchema.DuckDBTables;
import DT2.duckdb.ast.DuckDBConstant;
import DT2.duckdb.ast.DuckDBExpression;
import DT2.duckdb.ast.DuckDBJoin;
import DT2.duckdb.ast.DuckDBSelect;

public final class DuckDBRandomQuerySynthesizer {

    private DuckDBRandomQuerySynthesizer() {
    }

    public static DuckDBSelect generateSelect(DuckDBGlobalState globalState, int nrColumns) {
        DuckDBTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        DuckDBSelect select = new DuckDBSelect();
        // TODO: distinct
        // select.setDistinct(Randomly.getBoolean());
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<Node<DuckDBExpression>> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            Node<DuckDBExpression> expression = gen.generateExpression();
            columns.add(expression);
            // } else {
            // columns.add(gen());
            // }
        }
        select.setFetchColumns(columns);
        List<DuckDBTable> tables = targetTables.getTables();
        List<TableReferenceNode<DuckDBExpression, DuckDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DuckDBExpression, DuckDBTable>(t)).collect(Collectors.toList());
        List<Node<DuckDBExpression>> joins = DuckDBJoin.getJoins(tableList, globalState);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(DuckDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(
                    DuckDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        return select;
    }

}
