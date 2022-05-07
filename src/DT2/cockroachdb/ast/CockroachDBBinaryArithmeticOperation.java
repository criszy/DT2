package DT2.cockroachdb.ast;

import DT2.Randomly;
import DT2.cockroachdb.ast.CockroachDBBinaryArithmeticOperation.CockroachDBBinaryArithmeticOperator;
import DT2.common.ast.BinaryOperatorNode;
import DT2.common.ast.BinaryOperatorNode.Operator;

public class CockroachDBBinaryArithmeticOperation
        extends BinaryOperatorNode<CockroachDBExpression, CockroachDBBinaryArithmeticOperator>
        implements CockroachDBExpression {

    public enum CockroachDBBinaryArithmeticOperator implements Operator {
        ADD("+"), MULT("*"), MINUS("-"), DIV("/");

        String textRepresentation;

        CockroachDBBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static CockroachDBBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public CockroachDBBinaryArithmeticOperation(CockroachDBExpression left, CockroachDBExpression right,
            CockroachDBBinaryArithmeticOperator op) {
        super(left, right, op);
    }

}
