package DT2.tidb.ast;

import java.util.List;

import DT2.Randomly;
import DT2.common.ast.FunctionNode;
import DT2.tidb.ast.TiDBAggregate.TiDBAggregateFunction;

public class TiDBAggregate extends FunctionNode<TiDBAggregateFunction, TiDBExpression> implements TiDBExpression {

    public enum TiDBAggregateFunction {
        COUNT(1), //
        SUM(1), //
        AVG(1), //
        MIN(1), //
        MAX(1);

        private int nrArgs;

        TiDBAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static TiDBAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public TiDBAggregate(List<TiDBExpression> args, TiDBAggregateFunction func) {
        super(func, args);
    }

}
