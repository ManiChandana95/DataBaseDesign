package datatypes;

import common.Constant;
import datatypes.base.DataTypeNumeral;

public class DataTypeReal extends DataTypeNumeral<Float> {

    public DataTypeReal() {
        this(0, true);
    }

    public DataTypeReal(Float value) {
        this(value == null ? 0 : value, value == null);
    }

    public DataTypeReal(float value, boolean isNull) {
        super(Constant.REAL_SERIAL_TYPE_CODE, Constant.FOUR_BYTE_NULL_SERIAL_TYPE_CODE, Float.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Float value) {
        this.value += value;
    }

    @Override
    public boolean compare(DataTypeNumeral<Float> obj2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case DataTypeNumeral.EQUALS:
                return Float.floatToIntBits(value) == Float.floatToIntBits(obj2.getValue());

            case DataTypeNumeral.GREATER_THAN:
                return value > obj2.getValue();

            case DataTypeNumeral.LESS_THAN:
                return value < obj2.getValue();

            case DataTypeNumeral.GREATER_THAN_EQUALS:
                return Float.floatToIntBits(value) >= Float.floatToIntBits(obj2.getValue());

            case DataTypeNumeral.LESS_THAN_EQUALS:
                return Float.floatToIntBits(value) <= Float.floatToIntBits(obj2.getValue());

            default:
                return false;
        }
    }

    public boolean compare(DataTypeDouble obj2, short condition) {
        DataTypeDouble object = new DataTypeDouble(value, false);
        return object.compare(obj2, condition);
    }
}
