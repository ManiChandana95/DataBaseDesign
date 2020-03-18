package datatypes;

import common.Constant;
import datatypes.base.DataTypeNumeral;

public class DataTypeSmallInt extends DataTypeNumeral<Short> {

    public DataTypeSmallInt() {
        this((short) 0, true);
    }

    public DataTypeSmallInt(Short value) {
        this(value == null ? 0 : value, value == null);
    }

    public DataTypeSmallInt(short value, boolean isNull) {
        super(Constant.SMALL_INT_SERIAL_TYPE_CODE, Constant.TWO_BYTE_NULL_SERIAL_TYPE_CODE, Short.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Short value) {
        this.value = (short)(this.value + value);
    }

    @Override
    public boolean compare(DataTypeNumeral<Short> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case DataTypeNumeral.EQUALS:
                return value == object2.getValue();

            case DataTypeNumeral.GREATER_THAN:
                return value > object2.getValue();

            case DataTypeNumeral.LESS_THAN:
                return value < object2.getValue();

            case DataTypeNumeral.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case DataTypeNumeral.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }

    public boolean compare(DataTypeTinyInt object2, short condition) {
        DataTypeSmallInt object = new DataTypeSmallInt(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(DT_Int object2, short condition) {
        DT_Int object = new DT_Int(value, false);
        return object.compare(object2, condition);
    }

    public boolean compare(DataTypeInt object2, short condition) {
        DataTypeInt object = new DataTypeInt(value, false);
        return object.compare(object2, condition);
    }
}
