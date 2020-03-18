package datatypes;

import common.Constant;
import datatypes.base.DataTypeNumeral;


public class DT_Int extends DataTypeNumeral<Integer> {

    public DT_Int() {
        this(0, true);
    }

    public DT_Int(Integer value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_Int(int value, boolean isNull) {
        super(Constant.INT_SERIAL_TYPE_CODE, Constant.FOUR_BYTE_NULL_SERIAL_TYPE_CODE, Integer.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Integer value) {
        this.value += value;
    }

    @Override
    public boolean compare(DataTypeNumeral<Integer> obj2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case DataTypeNumeral.EQUALS:
                return value == obj2.getValue();

            case DataTypeNumeral.GREATER_THAN:
                return value > obj2.getValue();

            case DataTypeNumeral.LESS_THAN:
                return value < obj2.getValue();

            case DataTypeNumeral.GREATER_THAN_EQUALS:
                return value >= obj2.getValue();

            case DataTypeNumeral.LESS_THAN_EQUALS:
                return value <= obj2.getValue();

            default:
                return false;
        }
    }

    public boolean compare(DataTypeTinyInt object2, short condition) {
        DT_Int object = new DT_Int(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(DataTypeSmallInt object2, short condition) {
        DT_Int object = new DT_Int(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(DataTypeInt object2, short condition) {
        DataTypeInt object = new DataTypeInt(value, false);
        return object.compare(object2, condition);
    }
}
