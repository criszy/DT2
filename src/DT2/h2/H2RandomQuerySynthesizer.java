package DT2.h2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import DT2.Randomly;
import DT2.common.ast.newast.Node;
import DT2.common.ast.newast.TableReferenceNode;
import DT2.h2.H2Provider.H2GlobalState;
import DT2.h2.H2Schema.H2Table;
import DT2.h2.H2Schema.H2Tables;

public final class H2RandomQuerySynthesizer {

    private H2RandomQuerySynthesizer() {
    }

    public static H2Select generateSelect(H2GlobalState globalState, int nrColumns) {
        H2Tables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        H2ExpressionGenerator gen = new H2ExpressionGenerator(globalState).setColumns(targetTables.getColumns());
        H2Select select = new H2Select();
        List<Node<H2Expression>> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            Node<H2Expression> expression = gen.generateExpression();
            columns.add(expression);
        }
        select.setFetchColumns(columns);
        List<H2Table> tables = targetTables.getTables();
        List<TableReferenceNode<H2Expression, H2Table>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<H2Expression, H2Table>(t)).collect(Collectors.toList());
        List<Node<H2Expression>> joins = H2Join.getJoins(tableList, globalState);
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
            select.setLimitClause(H2Constant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(H2Constant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        return select;
    }

}
