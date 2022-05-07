package DT2.tidb.gen;

import java.sql.SQLException;
import java.util.List;

import DT2.common.query.ExpectedErrors;
import DT2.common.query.SQLQueryAdapter;
import DT2.mysql.gen.MySQLExpressionGenerator;
import DT2.tidb.TiDBExpressionGenerator;
import DT2.tidb.TiDBProvider.TiDBGlobalState;
import DT2.tidb.TiDBSchema.TiDBColumn;
import DT2.tidb.TiDBSchema.TiDBTable;
import DT2.tidb.visitor.TiDBVisitor;

public final class TiDBUpdateGenerator {

    private TiDBUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        ExpectedErrors errors = new ExpectedErrors();
        TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        List<TiDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
//            if (Randomly.getBoolean()) {
//                sb.append(gen.generateConstant());
//            } else {
//                sb.append(TiDBVisitor.asString(gen.generateExpression()));
//                TiDBErrors.addExpressionErrors(errors);
//            }
            String columnName = columns.get(i).getName();
            String columnType = TiDBTableGenerator.columnType.get(columnName);
            sb.append(MySQLExpressionGenerator.generateConstant(columnType));
        }
//        if (Randomly.getBoolean()) {
        if (true) {
            sb.append(" WHERE ");
//            TiDBErrors.addExpressionErrors(errors);
            sb.append(TiDBVisitor.asString(gen.generateExpression()));
//            errors.add("Data Too Long"); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/43
        }
//        TiDBErrors.addInsertErrors(errors);

        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
