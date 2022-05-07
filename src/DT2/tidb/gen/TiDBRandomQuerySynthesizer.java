package DT2.tidb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.query.SQLQueryAdapter;
import DT2.tidb.TiDBExpressionGenerator;
import DT2.tidb.TiDBProvider.TiDBGlobalState;
import DT2.tidb.TiDBSchema.TiDBTables;
import DT2.tidb.ast.TiDBExpression;
import DT2.tidb.ast.TiDBSelect;
import DT2.tidb.ast.TiDBTableReference;
import DT2.tidb.visitor.TiDBVisitor;

public final class TiDBRandomQuerySynthesizer {

    private TiDBRandomQuerySynthesizer() {
    }

    public static SQLQueryAdapter generate(TiDBGlobalState globalState, int nrColumns) {
        TiDBSelect select = generateSelect(globalState, nrColumns);
        return new SQLQueryAdapter(TiDBVisitor.asString(select));
    }

    public static TiDBSelect generateSelect(TiDBGlobalState globalState, int nrColumns) {
        TiDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(tables.getColumns());
        TiDBSelect select = new TiDBSelect();
        // select.setDistinct(Randomly.getBoolean());
        List<TiDBExpression> columns = new ArrayList<>();
        // TODO: also generate aggregates
        columns.addAll(gen.generateExpressions(nrColumns));
        select.setFetchColumns(columns);
        List<TiDBExpression> tableList = tables.getTables().stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        // TODO: generate joins
        select.setFromList(tableList);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(gen.generateExpression());
        }
        return select;
    }

}
