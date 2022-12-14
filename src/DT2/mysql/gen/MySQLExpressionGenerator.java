package DT2.mysql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import DT2.IgnoreMeException;
import DT2.Randomly;
import DT2.common.gen.UntypedExpressionGenerator;
import DT2.mysql.MySQLGlobalState;
import DT2.mysql.MySQLSchema.MySQLColumn;
import DT2.mysql.MySQLSchema.MySQLRowValue;
import DT2.mysql.ast.MySQLBinaryComparisonOperation;
import DT2.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;
import DT2.mysql.ast.MySQLBinaryLogicalOperation;
import DT2.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import DT2.mysql.ast.MySQLCastOperation;
import DT2.mysql.ast.MySQLColumnReference;
//import DT2.mysql.ast.MySQLComputableFunction;
//import DT2.mysql.ast.MySQLComputableFunction.MySQLFunction;
import DT2.mysql.ast.MySQLConstant;
import DT2.mysql.ast.MySQLConstant.MySQLDoubleConstant;
import DT2.mysql.ast.MySQLExists;
import DT2.mysql.ast.MySQLExpression;
import DT2.mysql.ast.MySQLInOperation;
import DT2.mysql.ast.MySQLOrderByTerm;
import DT2.mysql.ast.MySQLOrderByTerm.MySQLOrder;
import DT2.mysql.ast.MySQLStringExpression;
import DT2.mysql.ast.MySQLUnaryPostfixOperation;
import DT2.mysql.ast.MySQLUnaryPrefixOperation;
import DT2.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

public class MySQLExpressionGenerator extends UntypedExpressionGenerator<MySQLExpression, MySQLColumn> {

    private final MySQLGlobalState state;
    private MySQLRowValue rowVal;

    public MySQLExpressionGenerator(MySQLGlobalState state) {
        this.state = state;
    }

    public MySQLExpressionGenerator setRowVal(MySQLRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

//    private enum Actions {
//        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_POSTFIX, COMPUTABLE_FUNCTION, BINARY_LOGICAL_OPERATOR,
//        BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION, BINARY_OPERATION, EXISTS, BETWEEN_OPERATOR;
//    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, BINARY_LOGICAL_OPERATOR,
        BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION, EXISTS;
    }

    @Override
    public MySQLExpression generateExpression(int depth) {
        if (depth >= state.getOptions().getMaxExpressionDepth()) {
            return generateLeafNode();
        }
        switch (Randomly.fromOptions(Actions.values())) {
        case COLUMN:
            return generateColumn();
        case LITERAL:
            return generateConstant();
        case UNARY_PREFIX_OPERATION:
//            MySQLExpression subExpr = generateExpression(depth + 1);
            MySQLExpression subExpr = null;
            boolean isString = false;
            do {
                isString = false;
                subExpr = generateExpression(depth + 1);
                if(subExpr.toString().startsWith("\"") && subExpr.toString().endsWith("\"")){
                    isString = true;
                }
            } while(isString);
            MySQLUnaryPrefixOperator random = MySQLUnaryPrefixOperator.getRandom();
            boolean noError = false;
            while (!noError) {
                if (random == MySQLUnaryPrefixOperator.MINUS) {
                    random = MySQLUnaryPrefixOperator.getRandom();
                } else {
                    noError = true;
                }
            }
//            if (random == MySQLUnaryPrefixOperator.MINUS) {
//                // workaround for https://bugs.mysql.com/bug.php?id=99122
//                throw new IgnoreMeException();
//            }
            return new MySQLUnaryPrefixOperation(subExpr, random);
//        case UNARY_POSTFIX:
//            return new MySQLUnaryPostfixOperation(generateExpression(depth + 1),
//                    Randomly.fromOptions(MySQLUnaryPostfixOperation.UnaryPostfixOperator.values()),
//                    Randomly.getBoolean());
//        case COMPUTABLE_FUNCTION:
//            return getComputableFunction(depth + 1);
        case BINARY_LOGICAL_OPERATOR:
            return new MySQLBinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    MySQLBinaryLogicalOperator.getRandom());
        case BINARY_COMPARISON_OPERATION:
            return new MySQLBinaryComparisonOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    BinaryComparisonOperator.getRandom());
        case CAST:
            return new MySQLCastOperation(generateExpression(depth + 1), MySQLCastOperation.CastType.getRandom());
        case IN_OPERATION:
            MySQLExpression expr = generateExpression(depth + 1);
            List<MySQLExpression> rightList = new ArrayList<>();
            for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                rightList.add(generateExpression(depth + 1));
            }
            return new MySQLInOperation(expr, rightList, Randomly.getBoolean());
//        case BINARY_OPERATION:
//            if (MySQLBugs.bug99135) {
//                throw new IgnoreMeException();
//            }
//            return new MySQLBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
//                    MySQLBinaryOperator.getRandom());
        case EXISTS:
            return getExists();
//        case BETWEEN_OPERATOR:
//            if (MySQLBugs.bug99181) {
//                // TODO: there are a number of bugs that are triggered by the BETWEEN operator
//                throw new IgnoreMeException();
//            }
//            return new MySQLBetweenOperation(generateExpression(depth + 1), generateExpression(depth + 1),
//                    generateExpression(depth + 1));
        default:
            throw new AssertionError();
        }
    }

    private MySQLExpression getExists() {
        if (Randomly.getBoolean()) {
            return new MySQLExists(new MySQLStringExpression("SELECT 1", MySQLConstant.createTrue()));
        } else {
            return new MySQLExists(new MySQLStringExpression("SELECT 1 wHERE FALSE", MySQLConstant.createFalse()));
        }
    }

//    private MySQLExpression getComputableFunction(int depth) {
//        MySQLFunction func = MySQLFunction.getRandomFunction();
//        int nrArgs = func.getNrArgs();
//        if (func.isVariadic()) {
//            nrArgs += Randomly.smallNumber();
//        }
//        MySQLExpression[] args = new MySQLExpression[nrArgs];
//        for (int i = 0; i < args.length; i++) {
//            args[i] = generateExpression(depth + 1);
//        }
//        return new MySQLComputableFunction(func, args);
//    }

//    private enum ConstantType {
//        INT, NULL, STRING, DOUBLE;
//
//        public static ConstantType[] valuesPQS() {
//            return new ConstantType[] { INT, NULL, STRING };
//        }
//    }

    private enum ConstantType {
        INT, NULL, DOUBLE;

        public static ConstantType[] valuesPQS() {
            return new ConstantType[] { INT, NULL };
        }
    }

    @Override
    public MySQLExpression generateConstant() {
        ConstantType[] values;
        if (state.usesPQS()) {
            values = ConstantType.valuesPQS();
        } else {
            values = ConstantType.values();
        }
        switch (Randomly.fromOptions(values)) {
        case INT:
            return MySQLConstant.createIntConstant((int) state.getRandomly().getInteger());
        case NULL:
            return MySQLConstant.createNullConstant();
//        case STRING:
//            /* Replace characters that still trigger open bugs in MySQL */
//            String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
//            if (string.startsWith("\n")) {
//                // workaround for https://bugs.mysql.com/bug.php?id=99130
//                throw new IgnoreMeException();
//            }
//            if (string.startsWith("-0") || string.startsWith("0.") || string.startsWith(".")) {
//                // https://bugs.mysql.com/bug.php?id=99145
//                throw new IgnoreMeException();
//            }
//            MySQLConstant createStringConstant = MySQLConstant.createStringConstant(string);
//            // if (Randomly.getBoolean()) {
//            // return new MySQLCollate(createStringConstant,
//            // Randomly.fromOptions("ascii_bin", "binary"));
//            // }
//            if (string.startsWith("1e")) {
//                // https://bugs.mysql.com/bug.php?id=99146
//                throw new IgnoreMeException();
//            }
//            return createStringConstant;
        case DOUBLE:
            double val = state.getRandomly().getDouble();
            if (Math.abs(val) <= 1 && val != 0) {
                // https://bugs.mysql.com/bug.php?id=99145
                throw new IgnoreMeException();
            }
            if (Math.abs(val) > 1.0E30) {
                // https://bugs.mysql.com/bug.php?id=99146
                throw new IgnoreMeException();
            }
            return new MySQLDoubleConstant(val);
        default:
            throw new AssertionError();
        }
    }

    public static String generateConstant(String type){
        Random random = new Random(System.currentTimeMillis());
        switch (type) {
            case "BOOLEAN":
                return "" + (getRandomInt(-128, 127) > 0);
            case "TINYINT": //-128,127
                return "" + getRandomInt(-128, 127);
            case "SMALLINT": //-32768, 32767
            case "DECIMAL": //-9999999999,9999999999
                return "" + getRandomInt(-32768, 32767);
            case "MEDIUMINT": //-8388608, 8388607
                return "" + getRandomInt(-8388608, 8388607);
            case "INT": //-2147483648, 2147483647
            case "BIGINT": //big
                return "" + random.nextInt();
            case "FLOAT":
                return "" + random.nextFloat();
            case "DOUBLE":
                return "" + random.nextDouble();
            case "TINYTEXT":
                return getRandomString(5);
            case "BLOB":
            case "TEXT":
                return getRandomString(10);
            case "MEDIUMTEXT":
                return getRandomString(15);
            case "VARCHAR(500)":
            case "LONGTEXT":
                return getRandomString(20);
            default:
                return "NULL";
        }
    }

    private static int getRandomInt(int min, int max){
        Random random = new Random(System.currentTimeMillis());
        return Math.abs(random.nextInt(max - min + 1)) + min;
    }

    private static String getRandomString(int length){
        Random random = new Random(System.currentTimeMillis());
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuffer sb=new StringBuffer();
        sb.append("'");
        for(int i = 0; i < length; i++){
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        sb.append("'");
        return sb.toString();
    }

    @Override
    protected MySQLExpression generateColumn() {
        MySQLColumn c = Randomly.fromList(columns);
        MySQLConstant val;
        if (rowVal == null) {
            val = null;
        } else {
            val = rowVal.getValues().get(c);
        }
        return MySQLColumnReference.create(c, val);
    }

    @Override
    public MySQLExpression negatePredicate(MySQLExpression predicate) {
        return new MySQLUnaryPrefixOperation(predicate, MySQLUnaryPrefixOperator.NOT);
    }

    @Override
    public MySQLExpression isNull(MySQLExpression expr) {
        return new MySQLUnaryPostfixOperation(expr, MySQLUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<MySQLExpression> generateOrderBys() {
        List<MySQLExpression> expressions = super.generateOrderBys();
        List<MySQLExpression> newOrderBys = new ArrayList<>();
        for (MySQLExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                MySQLOrderByTerm newExpr = new MySQLOrderByTerm(expr, MySQLOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
