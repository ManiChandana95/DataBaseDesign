package datatypes;

import common.Constant;
import datatypes.base.DataTypeNumeral;

import java.text.SimpleDateFormat;
import java.util.Date;
public class DataTypeDate extends DataTypeNumeral<Long>{

    public DataTypeDate() {
        this(0, true);
    }

    public DataTypeDate(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    public DataTypeDate(long value, boolean isNull) {
        super(Constant.DATE_SERIAL_TYPE_CODE, Constant.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Long.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    public String getStringValue() {
        Date date = new Date(this.value);
        return new SimpleDateFormat("MM-dd-yyyy").format(date);
    }

    @Override
    public void increment(Long value) {
        this.value += value;
    }

    @Override
    public boolean compare(DataTypeNumeral<Long> object2, short condition) {
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
}
