package DT2.postgres.ast;

import DT2.postgres.PostgresSchema.PostgresDataType;

public interface PostgresExpression {

    default PostgresDataType getExpressionType() {
        return null;
    }

    default PostgresConstant getExpectedValue() {
        return null;
    }
}
