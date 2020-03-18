package datatypes;

import common.Constant;
import datatypes.base.DataTypeNumeral;

public class DataTypeTinyInt extends DataTypeNumeral<Byte> {

    public DataTypeTinyInt() {
        this((byte) 0, true);
    }

    public DataTypeTinyInt(Byte value) {
        this(value == null ? 0 : value, value == null);
    }

    public DataTypeTinyInt(byte value, boolean isNull) {
        super(Constant.TINY_INT_SERIAL_TYPE_CODE, Constant.ONE_BYTE_NULL_SERIAL_TYPE_CODE, Byte.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Byte value) {
        this.value = (byte)(this.value + value);
    }

    @Override
    public boolean compare(DataTypeNumeral<Byte> object2, short condition) {
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

    public boolean compare(DataTypeSmallInt object2, short condition) {
        DataTypeSmallInt object = new DataTypeSmallInt(value, false);
        return object.compare(object2, condition);
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
