package DT2.oceanbase.ast;

import DT2.oceanbase.OceanBaseSchema;

public class OceanBaseColumnName implements OceanBaseExpression {

    private final OceanBaseSchema.OceanBaseColumn column;

    public OceanBaseColumnName(OceanBaseSchema.OceanBaseColumn column) {
        this.column = column;
    }

    public OceanBaseSchema.OceanBaseColumn getColumn() {
        return column;
    }

}
