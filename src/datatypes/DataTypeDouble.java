package datatypes;

import common.Constant;
import datatypes.base.DataTypeNumeral;
public class DataTypeDouble extends DataTypeNumeral<Double> {

    public DataTypeDouble() {
        this(0, true);
    }

    public DataTypeDouble(Double value) {
        this(value == null ? 0 : value, value == null);
    }

    public DataTypeDouble(double value, boolean isNull) {
        super(Constant.DOUBLE_SERIAL_TYPE_CODE, Constant.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Double.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Double value) {
        this.value += value;
    }

    @Override
    public boolean compare(DataTypeNumeral<Double> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case DataTypeNumeral.EQUALS:
                return Double.doubleToLongBits(value) == Double.doubleToLongBits(object2.getValue());

            case DataTypeNumeral.GREATER_THAN:
                return value > object2.getValue();

            case DataTypeNumeral.LESS_THAN:
                return value < object2.getValue();

            case DataTypeNumeral.GREATER_THAN_EQUALS:
                return Double.doubleToLongBits(value) >= Double.doubleToLongBits(object2.getValue());

            case DataTypeNumeral.LESS_THAN_EQUALS:
                return Double.doubleToLongBits(value) <= Double.doubleToLongBits(object2.getValue());

            default:
                return false;
        }
    }

    public boolean compare(DataTypeReal object2, short condition) {
        DataTypeDouble object = new DataTypeDouble(object2.getValue(), false);
        return this.compare(object, condition);
    }

}
