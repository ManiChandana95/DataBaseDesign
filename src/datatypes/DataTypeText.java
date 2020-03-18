package datatypes;

import common.Constant;
import datatypes.base.DT;

public class DataTypeText extends DT<String> {

    public DataTypeText() {
        this("", true);
    }

    public DataTypeText(String value) {
        this(value, value == null);
    }

    public DataTypeText(String value, boolean isNull) {
        super(Constant.TEXT_SERIAL_TYPE_CODE, Constant.ONE_BYTE_NULL_SERIAL_TYPE_CODE);
        this.value = value;
        this.isNull = isNull;
    }

    public byte getSerialCode() {
        if(isNull)
            return nullSerialCode;
        else
            return (byte)(valueSerialCode + this.value.length());
    }

    public int getSize() {
        if(isNull)
            return 0;
        return this.value.length();
    }
}
