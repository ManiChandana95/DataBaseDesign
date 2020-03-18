package datatypes;

import common.Constant;
import datatypes.base.DataTypeNumeral;


public class DataTypeInt extends DataTypeNumeral<Long> {

    public DataTypeInt() {
        this(0, true);
    }

 

    public DataTypeInt(long value, boolean isNull) {
        super(Constant.BIG_INT_SERIAL_TYPE_CODE, Constant.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Long.BYTES);
        this.value = value;
        this.isNull = isNull;
    }
    
    public DataTypeInt(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    @Override
    public void increment(Long value) {
        this.value += value;
    }

    @Override
    public boolean compare(DataTypeNumeral<Long> obj2, short condition) {
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

    public boolean compare(DataTypeTinyInt obj2, short condition) {
        DataTypeInt object = new DataTypeInt(obj2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(DataTypeSmallInt obj2, short condition) {
        DataTypeInt object = new DataTypeInt(obj2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(DT_Int obj2, short condition) {
        DataTypeInt object = new DataTypeInt(obj2.getValue(), false);
        return this.compare(object, condition);
    }
}
