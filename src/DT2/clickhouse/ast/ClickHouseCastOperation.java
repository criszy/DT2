package DT2.clickhouse.ast;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import DT2.clickhouse.ClickHouseSchema.ClickHouseLancerDataType;

public class ClickHouseCastOperation extends ClickHouseExpression {

    private final ClickHouseExpression expression;
    private final ClickHouseLancerDataType type;

    public ClickHouseCastOperation(ClickHouseExpression expression, ClickHouseLancerDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    @Override
    public ClickHouseConstant getExpectedValue() {
        return expression.getExpectedValue().cast(type.getType());
    }

    public ClickHouseExpression getExpression() {
        return expression;
    }

    public ClickHouseDataType getType() {
        return type.getType();
    }

    public ClickHouseLancerDataType getCompoundType() {
        return type;
    }

}
