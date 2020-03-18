package storage;

import common.CatalogDatabase;
import common.Constant;
import common.Utils;
import console.ConsoleWriter;
import datatypes.*;
import datatypes.base.DT;
import datatypes.base.DataTypeNumeral;
import helpers.UpdateStmtHandler;
import storage.model.AllDataRecords;
import storage.model.InternalCondition;
import storage.model.Page;
import storage.model.AllPointerRecord;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StorageManager {

    private String DEFAULT_DATA_PATH = Constant.DEFAULT_DATA_DIRNAME;

    public boolean createDatabaseQuery(String databaseName) {
        try {
            File dirFile = new File(DEFAULT_DATA_PATH + "/" + databaseName);
            if (dirFile.exists()) {
                return false;
            }
            return dirFile.mkdirs();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean dropDatabaseQuery(String dbName) {
        try {
            File dirFile = new File(DEFAULT_DATA_PATH + "/" + dbName);
            if (!dirFile.exists()) {
                System.out.println("Database " + dbName + " doesn't!");
                return false;
            }
            return dirFile.mkdirs();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean IsdatabaseExists(String dbName) {
        File databaseDir = new File(dbName);
        return  databaseDir.exists();
    }

    public boolean createTable(String dbName, String tbName) {
        try {
            File dirFile = new File(dbName);
            if (!dirFile.exists()) {
                dirFile.mkdir();
            }
            File file = new File(dbName + "/" + tbName);
            if (file.exists()) {
                return false;
            }
            if (file.createNewFile()) {
                RandomAccessFile randomAccessFile;
                Page<AllDataRecords> page = Page.createNewEmptyPage(new AllDataRecords());
                randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(Page.PAGE_SIZE);
                boolean isTableCreated = writePageHeader(randomAccessFile, page);
                randomAccessFile.close();
                return isTableCreated;
            }
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

   
    public boolean checkTableExists(String databaseName, String tableName) {
        boolean IsdatabaseExists = this.IsdatabaseExists(databaseName);
        boolean IsfileExists = new File(databaseName + "/" + tableName + Constant.DEFAULT_FILE_EXTENSION).exists();

        return (IsdatabaseExists && IsfileExists);
    }

    public boolean writeRecord(String databaseName, String tableName, AllDataRecords record) {
        RandomAccessFile randomAccessFile = null;
        try {
            File file = new File(databaseName + "/" + tableName + Constant.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                randomAccessFile = new RandomAccessFile(file, "rw");
                Page page = getPage(randomAccessFile, record, 0);
                if (page == null) return false;
                if (!checkSpaceRequirements(page, record)) {
                    int pageCount = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                    switch (pageCount) {
                        case 1:
                            AllPointerRecord pointerRecord = splitPage(randomAccessFile, page, record, 1, 2);
                            Page<AllPointerRecord> pointerRecordPage = Page.createNewEmptyPage(pointerRecord);
                            pointerRecordPage.setPageNumber(0);
                            pointerRecordPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                            pointerRecordPage.setNumberOfCells((byte) 1);
                            pointerRecordPage.setStartingAddress((short) (pointerRecordPage.getStartingAddress() - pointerRecord.getSize()));
                            pointerRecordPage.setRightNodeAddress(2);
                            pointerRecordPage.getRecordAddressList().add((short) (pointerRecordPage.getStartingAddress() + 1));
                            pointerRecord.setPageNumber(pointerRecordPage.getPageNumber());
                            pointerRecord.setOffset((short) (pointerRecordPage.getStartingAddress() + 1));
                            this.writePageHeader(randomAccessFile, pointerRecordPage);
                            this.writeRecord(randomAccessFile, pointerRecord);
                            break;

                        default:
                            if(pageCount > 1) {
                                AllPointerRecord pointerRecord1 = splitPage(randomAccessFile, readPageHeader(randomAccessFile, 0), record);
                                if(pointerRecord1 != null && pointerRecord1.getLeftPageNumber() != -1)  {
                                    Page<AllPointerRecord> rootPage = Page.createNewEmptyPage(pointerRecord1);
                                    rootPage.setPageNumber(0);
                                    rootPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                                    rootPage.setNumberOfCells((byte) 1);
                                    rootPage.setStartingAddress((short)(rootPage.getStartingAddress() - pointerRecord1.getSize()));
                                    rootPage.setRightNodeAddress(pointerRecord1.getPageNumber());
                                    rootPage.getRecordAddressList().add((short) (rootPage.getStartingAddress() + 1));
                                    pointerRecord1.setOffset((short) (rootPage.getStartingAddress() + 1));
                                    this.writePageHeader(randomAccessFile, rootPage);
                                    this.writeRecord(randomAccessFile, pointerRecord1);
                                }
                            }
                            break;
                    }
                    UpdateStmtHandler.incrementRowCount(tableName);
                    randomAccessFile.close();
                    return true;
                }
                short address = (short) getAddress(file, record.getRowIdx(), page.getPageNumber());
                page.setNumberOfCells((byte)(page.getNumberOfCells() + 1));
                page.setStartingAddress((short) (page.getStartingAddress() - record.getSize() - record.getHeaderSize()));
                if(address == page.getRecordAddressList().size())
                    page.getRecordAddressList().add((short)(page.getStartingAddress() + 1));
                else
                    page.getRecordAddressList().add(address, (short)(page.getStartingAddress() + 1));
                record.setPgLocated(page.getPageNumber());
                record.setOffset((short) (page.getStartingAddress() + 1));
                this.writePageHeader(randomAccessFile, page);
                this.writeRecord(randomAccessFile, record);
                randomAccessFile.close();
            } else {
                ConsoleWriter.displayMessage("File " + tableName + " does not exist");
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean checkSpaceRequirements(Page page, AllDataRecords record) {
        if (page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSize() + record.getHeaderSize() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    private boolean checkSpaceRequirements(Page page, AllPointerRecord record) {
        if(page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSize() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    private AllPointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, AllDataRecords record, int pageNumber1, int pageNumber2) {
        try {
            if (page != null && record != null) {
                int location;
                AllPointerRecord pointerRecord = new AllPointerRecord();
                if (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getRowIdx(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType());
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));
                if (location == page.getNumberOfCells()) {
                    Page<AllDataRecords> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setNumberOfCells(page.getNumberOfCells());
                    page1.setRightNodeAddress(pageNumber2);
                    page1.setStartingAddress(page.getStartingAddress());
                    page1.setRecordAddressList(page.getRecordAddressList());
                    this.writePageHeader(randomAccessFile, page1);
                    List<AllDataRecords> records = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, page.getNumberOfCells(), page1.getPageNumber(), record);
                    for (AllDataRecords object : records) {
                        this.writeRecord(randomAccessFile, object);
                    }
                    Page<AllDataRecords> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    page2.setNumberOfCells((byte) 1);
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    page2.setStartingAddress((short) (page2.getStartingAddress() - record.getSize() - record.getHeaderSize()));
                    page2.getRecordAddressList().add((short) (page2.getStartingAddress() + 1));
                    this.writePageHeader(randomAccessFile, page2);
                    record.setPgLocated(page2.getPageNumber());
                    record.setOffset((short) (page2.getStartingAddress() + 1));
                    this.writeRecord(randomAccessFile, record);
                    pointerRecord.setKey(record.getRowIdx());
                } else {
                    boolean isFirst = false;
                    if (location < (page.getRecordAddressList().size() / 2)) {
                        isFirst = true;
                    }
                    randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));

  
                    Page<AllDataRecords> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setPageNumber(pageNumber1);
                    List<AllDataRecords> leftRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, (byte) (page.getNumberOfCells() / 2), page1.getPageNumber(), record);
                    if (isFirst)
                        leftRecords.add(location, record);
                    page1.setNumberOfCells((byte) leftRecords.size());
                    int index = 0;
                    short offset = (short) (Page.PAGE_SIZE - 1);
                    for (AllDataRecords dataRecord : leftRecords) {
                        index++;
                        offset = (short) (Page.PAGE_SIZE - ((dataRecord.getSize() + dataRecord.getHeaderSize()) * index));
                        dataRecord.setOffset(offset);
                        page1.getRecordAddressList().add(offset);
                    }
                    page1.setStartingAddress((short) (offset + 1));
                    page1.setRightNodeAddress(pageNumber2);
                    this.writePageHeader(randomAccessFile, page1);
                    for(AllDataRecords dataRecord : leftRecords) {
                        this.writeRecord(randomAccessFile, dataRecord);
                    }

                 
                    Page<AllDataRecords> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    List<AllDataRecords> rightRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) ((page.getNumberOfCells() / 2) + 1), page.getNumberOfCells(), pageNumber2, record);
                    if(!isFirst) {
                        int position = (location - (page.getRecordAddressList().size() / 2) + 1);
                        if(position >= rightRecords.size())
                            rightRecords.add(record);
                        else
                            rightRecords.add(position, record);
                    }
                    page2.setNumberOfCells((byte) rightRecords.size());
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    pointerRecord.setKey(rightRecords.get(0).getRowIdx());
                    index = 0;
                    offset = (short) (Page.PAGE_SIZE - 1);
                    for(AllDataRecords dataRecord : rightRecords) {
                        index++;
                        offset = (short) (Page.PAGE_SIZE - ((dataRecord.getSize() + dataRecord.getHeaderSize()) * index));
                        dataRecord.setOffset(offset);
                        page2.getRecordAddressList().add(offset);
                    }
                    page2.setStartingAddress((short) (offset + 1));
                    this.writePageHeader(randomAccessFile, page2);
                    for(AllDataRecords dataRecord : rightRecords) {
                        this.writeRecord(randomAccessFile, dataRecord);
                    }
                }
                pointerRecord.setLeftPageNumber(pageNumber1);
                return pointerRecord;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private AllPointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, AllDataRecords record) {
        if(page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
            int pageNumber = binarySearch(randomAccessFile, record.getRowIdx(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
            Page newPage = this.readPageHeader(randomAccessFile, pageNumber);
            AllPointerRecord pointerRecord = splitPage(randomAccessFile, newPage, record);
            if(pointerRecord.getPageNumber() == -1)
                return pointerRecord;
            if(checkSpaceRequirements(page, pointerRecord)) {
                int location = binarySearch(randomAccessFile, record.getRowIdx(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE, true);
                page.setNumberOfCells((byte) (page.getNumberOfCells() + 1));
                page.setStartingAddress((short) (page.getStartingAddress() - pointerRecord.getSize()));
                page.getRecordAddressList().add(location, (short)(page.getStartingAddress() + 1));
                page.setRightNodeAddress(pointerRecord.getPageNumber());
                pointerRecord.setPageNumber(page.getPageNumber());
                pointerRecord.setOffset((short) (page.getStartingAddress() + 1));
                this.writePageHeader(randomAccessFile, page);
                this.writeRecord(randomAccessFile, pointerRecord);
                return new AllPointerRecord();
            }
            else {
                int newPageNumber;
                try {
                    newPageNumber = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
                page.setRightNodeAddress(pointerRecord.getPageNumber());
                this.writePageHeader(randomAccessFile, page);
                AllPointerRecord pointerRecord1;
                pointerRecord1 = splitPage(randomAccessFile, page, pointerRecord, page.getPageNumber(), newPageNumber);
                return pointerRecord1;
            }
        }
        else if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
            int newPageNumber;
            try {
                newPageNumber = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
            }
            catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            AllPointerRecord pointerRecord = splitPage(randomAccessFile, page, record, page.getPageNumber(), newPageNumber);
            if(pointerRecord != null)
                pointerRecord.setPageNumber(newPageNumber);
            return pointerRecord;
        }
        return null;
    }

    private AllPointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, AllPointerRecord record, int pageNumber1, int pageNumber2) {
        try {
            if (page != null && record != null) {
                int location;
                boolean isFirst = false;

                AllPointerRecord pointerRecord;
                if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getKey(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType(), true);
                if (location < (page.getRecordAddressList().size() / 2)) {
                    isFirst = true;
                }

                if(pageNumber1 == 0) {
                    pageNumber1 = pageNumber2;
                    pageNumber2++;
                }
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));

            
                Page<AllPointerRecord> page1 = new Page<>(pageNumber1);
                page1.setPageType(page.getPageType());
                page1.setPageNumber(pageNumber1);
                List<AllPointerRecord> leftRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, (byte) (page.getNumberOfCells() / 2), page1.getPageNumber(), record);
                if (isFirst)
                    leftRecords.add(location, record);
                pointerRecord = leftRecords.get(leftRecords.size() - 1);
                pointerRecord.setPageNumber(pageNumber2);
                leftRecords.remove(leftRecords.size() - 1);
                page1.setNumberOfCells((byte) leftRecords.size());
                int index = 0;
                short offset = (short) (Page.PAGE_SIZE - 1);
                for (AllPointerRecord pointerRecord1 : leftRecords) {
                    index++;
                    offset = (short) (Page.PAGE_SIZE - (pointerRecord1.getSize() * index));
                    pointerRecord1.setOffset(offset);
                    page1.getRecordAddressList().add(offset);
                }
                page1.setStartingAddress((short) (offset + 1));
                page1.setRightNodeAddress(pointerRecord.getLeftPageNumber());
                this.writePageHeader(randomAccessFile, page1);
                for(AllPointerRecord pointerRecord1 : leftRecords) {
                    this.writeRecord(randomAccessFile, pointerRecord1);
                }

        
                Page<AllPointerRecord> page2 = new Page<>(pageNumber2);
                page2.setPageType(page.getPageType());
                List<AllPointerRecord> rightRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) ((page.getNumberOfCells() / 2) + 1), page.getNumberOfCells(), pageNumber2, record);
                if(!isFirst) {
                    int position = (location - (page.getRecordAddressList().size() / 2) + 1);
                    if(position >= rightRecords.size())
                        rightRecords.add(record);
                    else
                        rightRecords.add(position, record);
                }
                page2.setNumberOfCells((byte) rightRecords.size());
                page2.setRightNodeAddress(page.getRightNodeAddress());
                rightRecords.get(0).setLeftPageNumber(page.getRightNodeAddress());
                index = 0;
                offset = (short) (Page.PAGE_SIZE - 1);
                for(AllPointerRecord pointerRecord1 : rightRecords) {
                    index++;
                    offset = (short) (Page.PAGE_SIZE - (pointerRecord1.getSize() * index));
                    pointerRecord1.setOffset(offset);
                    page2.getRecordAddressList().add(offset);
                }
                page2.setStartingAddress((short) (offset + 1));
                this.writePageHeader(randomAccessFile, page2);
                for(AllPointerRecord pointerRecord1 : rightRecords) {
                    this.writeRecord(randomAccessFile, pointerRecord1);
                }
                pointerRecord.setPageNumber(pageNumber2);
                pointerRecord.setLeftPageNumber(pageNumber1);
                return pointerRecord;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private <T> List<T> copyRecords(RandomAccessFile randomAccessFile, long pageStartAddress, List<Short> recordAddresses, byte startIndex, byte endIndex, int pageNumber, T object) {
        try {
            List<T> records = new ArrayList<>();
            byte numberOfRecords;
            byte[] serialTypeCodes;
            for (byte i = startIndex; i < endIndex; i++) {
                randomAccessFile.seek(pageStartAddress + recordAddresses.get(i));
                if (object.getClass().equals(AllPointerRecord.class)) {
                    AllPointerRecord record = new AllPointerRecord();
                    record.setPageNumber(pageNumber);
                    record.setOffset((short) (pageStartAddress + Page.PAGE_SIZE - 1 - (record.getSize() * (i - startIndex + 1))));
                    record.setLeftPageNumber(randomAccessFile.readInt());
                    record.setKey(randomAccessFile.readInt());
                    records.add(i - startIndex, (T) record);
                } else if (object.getClass().equals(AllDataRecords.class)) {
                    AllDataRecords record = new AllDataRecords();
                    record.setPgLocated(pageNumber);
                    record.setOffset(recordAddresses.get(i));
                    record.setSize(randomAccessFile.readShort());
                    record.setRowIdx(randomAccessFile.readInt());
                    numberOfRecords = randomAccessFile.readByte();
                    serialTypeCodes = new byte[numberOfRecords];
                    for (byte j = 0; j < numberOfRecords; j++) {
                        serialTypeCodes[j] = randomAccessFile.readByte();
                    }
                    for (byte j = 0; j < numberOfRecords; j++) {
                        switch (serialTypeCodes[j]) {
                            case Constant.ONE_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeText(null));
                                break;

                            case Constant.TWO_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeSmallInt(randomAccessFile.readShort(), true));
                                break;

                            case Constant.FOUR_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeReal(randomAccessFile.readFloat(), true));
                                break;

                            case Constant.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeDouble(randomAccessFile.readDouble(), true));
                                break;

                            case Constant.TINY_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeTinyInt(randomAccessFile.readByte()));
                                break;

                            case Constant.SMALL_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeSmallInt(randomAccessFile.readShort()));
                                break;

                            case Constant.INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Int(randomAccessFile.readInt()));
                                break;

                            case Constant.BIG_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeInt(randomAccessFile.readLong()));
                                break;

                            case Constant.REAL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeReal(randomAccessFile.readFloat()));
                                break;

                            case Constant.DOUBLE_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeDouble(randomAccessFile.readDouble()));
                                break;

                            case Constant.DATE_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeDate(randomAccessFile.readLong()));
                                break;

                            case Constant.DATE_TIME_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeDateTime(randomAccessFile.readLong()));
                                break;

                            case Constant.TEXT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeText(""));
                                break;

                            default:
                                if (serialTypeCodes[j] > Constant.TEXT_SERIAL_TYPE_CODE) {
                                    byte length = (byte) (serialTypeCodes[j] - Constant.TEXT_SERIAL_TYPE_CODE);
                                    char[] text = new char[length];
                                    for (byte k = 0; k < length; k++) {
                                        text[k] = (char) randomAccessFile.readByte();
                                    }
                                    record.getColumnValueList().add(new DataTypeText(new String(text)));
                                }
                                break;

                        }
                    }
                    records.add(i - startIndex, (T) record);
                }
            }
            return records;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Page getPage(RandomAccessFile randomAccessFile, AllDataRecords record, int pageNumber) {
        try {
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if (page.getPageType() == Page.LEAF_TABLE_PAGE) {
                return page;
            }
            pageNumber = binarySearch(randomAccessFile, record.getRowIdx(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
            if (pageNumber == -1) return null;
            return getPage(randomAccessFile, record, pageNumber);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private int getAddress(File file, int rowId, int pageNumber) {
        int location = -1;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                location = binarySearch(randomAccessFile, rowId, page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.LEAF_TABLE_PAGE);
                randomAccessFile.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return location;
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType) {
        return binarySearch(randomAccessFile, key, numberOfRecords, seekPosition, pageType, false);
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType, boolean literalSearch) {
        try {
            int start = 0, end = numberOfRecords;
            int mid;
            int pageNumber = -1;
            int rowId;
            short address;

            while(true) {
                if(start > end || start == numberOfRecords) {
                    if(pageType == Page.LEAF_TABLE_PAGE || literalSearch)
                        return start > numberOfRecords ? numberOfRecords : start;
                    if(pageType == Page.INTERIOR_TABLE_PAGE) {
                        if (end < 0)
                            return pageNumber;
                        randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + 4);
                        return randomAccessFile.readInt();
                    }
                }
                mid = (start + end) / 2;
                randomAccessFile.seek(seekPosition + (Short.BYTES * mid));
                address = randomAccessFile.readShort();
                randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + address);
                if (pageType == Page.LEAF_TABLE_PAGE) {
                    randomAccessFile.readShort();
                    rowId = randomAccessFile.readInt();
                    if (rowId == key) return mid;
                    if (rowId > key) {
                        end = mid - 1;
                    } else {
                        start = mid + 1;
                    }
                } else if (pageType == Page.INTERIOR_TABLE_PAGE) {
                    pageNumber = randomAccessFile.readInt();
                    rowId = randomAccessFile.readInt();
                    if (rowId > key) {
                        end = mid - 1;
                    } else {
                        start = mid + 1;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    private Page readPageHeader(RandomAccessFile randomAccessFile, int pageNumber) {
        try {
            Page page;
            randomAccessFile.seek(Page.PAGE_SIZE * pageNumber);
            byte pageType = randomAccessFile.readByte();
            if (pageType == Page.INTERIOR_TABLE_PAGE) {
                page = new Page<AllPointerRecord>();
            } else {
                page = new Page<AllDataRecords>();
            }
            page.setPageType(pageType);
            page.setPageNumber(pageNumber);
            page.setNumberOfCells(randomAccessFile.readByte());
            page.setStartingAddress(randomAccessFile.readShort());
            page.setRightNodeAddress(randomAccessFile.readInt());
            for (byte i = 0; i < page.getNumberOfCells(); i++) {
                page.getRecordAddressList().add(randomAccessFile.readShort());
            }
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private boolean writePageHeader(RandomAccessFile randomAccessFile, Page page) {
        try {
            randomAccessFile.seek(page.getPageNumber() * Page.PAGE_SIZE);
            randomAccessFile.writeByte(page.getPageType());
            randomAccessFile.writeByte(page.getNumberOfCells());
            randomAccessFile.writeShort(page.getStartingAddress());
            randomAccessFile.writeInt(page.getRightNodeAddress());
            for (Object offset : page.getRecordAddressList()) {
                randomAccessFile.writeShort((short) offset);
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, AllDataRecords record) {
        try {
            randomAccessFile.seek((record.getPgLocated() * Page.PAGE_SIZE) + record.getOffset());
            randomAccessFile.writeShort(record.getSize());
            randomAccessFile.writeInt(record.getRowIdx());
            randomAccessFile.writeByte((byte) record.getColumnValueList().size());
            randomAccessFile.write(record.getSerialTypeCodes());
            for (Object object : record.getColumnValueList()) {
                switch (Utils.resolveClass(object)) {
                    case Constant.TINYINT:
                        randomAccessFile.writeByte(((DataTypeTinyInt) object).getValue());
                        break;

                    case Constant.SMALLINT:
                        randomAccessFile.writeShort(((DataTypeSmallInt) object).getValue());
                        break;

                    case Constant.INT:
                        randomAccessFile.writeInt(((DT_Int) object).getValue());
                        break;

                    case Constant.BIGINT:
                        randomAccessFile.writeLong(((DataTypeInt) object).getValue());
                        break;

                    case Constant.REAL:
                        randomAccessFile.writeFloat(((DataTypeReal) object).getValue());
                        break;

                    case Constant.DOUBLE:
                        randomAccessFile.writeDouble(((DataTypeDouble) object).getValue());
                        break;

                    case Constant.DATE:
                        randomAccessFile.writeLong(((DataTypeDate) object).getValue());
                        break;

                    case Constant.DATETIME:
                        randomAccessFile.writeLong(((DataTypeDateTime) object).getValue());
                        break;

                    case Constant.TEXT:
                        if (((DataTypeText) object).getValue() != null)
                            randomAccessFile.writeBytes(((DataTypeText) object).getValue());
                        break;

                    default:
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, AllPointerRecord record) {
        try {
            randomAccessFile.seek((record.getPageNumber() * Page.PAGE_SIZE) + record.getOffset());
            randomAccessFile.writeInt(record.getLeftPageNumber());
            randomAccessFile.writeInt(record.getKey());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public List<AllDataRecords> findRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, boolean getOne) {
        return findRecord(databaseName, tableName, columnIndexList, valueList, conditionList, null, getOne);
    }

    public List<AllDataRecords> findRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, List<Byte> selectionColumnIndexList, boolean getOne) {
        List<InternalCondition> conditions = new ArrayList<>();
        for (byte i = 0; i < columnIndexList.size(); i++) {
            conditions.add(InternalCondition.CreateCondition(columnIndexList.get(i), conditionList.get(i), valueList.get(i)));
        }
        return findRecord(databaseName, tableName, conditions, selectionColumnIndexList, getOne);
    }

    public List<AllDataRecords> findRecord(String databaseName, String tableName, InternalCondition condition, boolean getOne) {
        return findRecord(databaseName, tableName, condition,null, getOne);
    }

    public List<AllDataRecords> findRecord(String databaseName, String tableName, InternalCondition condition, List<Byte> selectionColumnIndexList, boolean getOne) {
        List<InternalCondition> conditionList = new ArrayList<>();
        if(condition != null)
            conditionList.add(condition);
        return findRecord(databaseName, tableName, conditionList, selectionColumnIndexList, getOne);
    }

    public List<AllDataRecords> findRecord(String databaseName, String tableName, List<InternalCondition> conditionList, boolean getOne) {
        return findRecord(databaseName, tableName, conditionList, null, getOne);
    }

    public List<AllDataRecords> findRecord(String databaseName, String tableName, List<InternalCondition> conditionList, List<Byte> selectionColumnIndexList, boolean getOne) {
        try {
            File file = new File(databaseName + "/" + tableName + Constant.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                if (conditionList != null) {
                    Page page = getFirstPage(file);
                    AllDataRecords record;
                    List<AllDataRecords> matchRecords = new ArrayList<>();
                    boolean isMatch = false;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Object offset : page.getRecordAddressList()) {
                            isMatch = true;
                            record = getDataRecord(randomAccessFile, page.getPageNumber(), (short) offset);
                            for(int i = 0; i < conditionList.size(); i++) {
                                isMatch = false;
                                columnIndex = conditionList.get(i).getIndex();
                                value = conditionList.get(i).getValue();
                                condition = conditionList.get(i).getConditionType();
                                if (record != null && record.getColumnValueList().size() > columnIndex) {
                                    Object object = record.getColumnValueList().get(columnIndex);
                                    switch (Utils.resolveClass(object)) {
                                        case Constant.TINYINT:
                                            switch (Utils.resolveClass(value)) {
                                                case Constant.TINYINT:
                                                    isMatch = ((DataTypeTinyInt) object).compare((DataTypeTinyInt) value, condition);
                                                    break;

                                                case Constant.SMALLINT:
                                                    isMatch = ((DataTypeTinyInt) object).compare((DataTypeSmallInt) value, condition);
                                                    break;

                                                case Constant.INT:
                                                    isMatch = ((DataTypeTinyInt) object).compare((DT_Int) value, condition);
                                                    break;

                                                case Constant.BIGINT:
                                                    isMatch = ((DataTypeTinyInt) object).compare((DataTypeInt) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constant.SMALLINT:
                                            switch (Utils.resolveClass(value)) {
                                                case Constant.TINYINT:
                                                    isMatch = ((DataTypeSmallInt) object).compare((DataTypeTinyInt) value, condition);
                                                    break;

                                                case Constant.SMALLINT:
                                                    isMatch = ((DataTypeSmallInt) object).compare((DataTypeSmallInt) value, condition);
                                                    break;

                                                case Constant.INT:
                                                    isMatch = ((DataTypeSmallInt) object).compare((DT_Int) value, condition);
                                                    break;

                                                case Constant.BIGINT:
                                                    isMatch = ((DataTypeSmallInt) object).compare((DataTypeInt) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constant.INT:
                                            switch (Utils.resolveClass(value)) {
                                                case Constant.TINYINT:
                                                    isMatch = ((DT_Int) object).compare((DataTypeTinyInt) value, condition);
                                                    break;

                                                case Constant.SMALLINT:
                                                    isMatch = ((DT_Int) object).compare((DataTypeSmallInt) value, condition);
                                                    break;

                                                case Constant.INT:
                                                    isMatch = ((DT_Int) object).compare((DT_Int) value, condition);
                                                    break;

                                                case Constant.BIGINT:
                                                    isMatch = ((DT_Int) object).compare((DataTypeInt) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constant.BIGINT:
                                            switch (Utils.resolveClass(value)) {
                                                case Constant.TINYINT:
                                                    isMatch = ((DataTypeInt) object).compare((DataTypeTinyInt) value, condition);
                                                    break;

                                                case Constant.SMALLINT:
                                                    isMatch = ((DataTypeInt) object).compare((DataTypeSmallInt) value, condition);
                                                    break;

                                                case Constant.INT:
                                                    isMatch = ((DataTypeInt) object).compare((DT_Int) value, condition);
                                                    break;

                                                case Constant.BIGINT:
                                                    isMatch = ((DataTypeInt) object).compare((DataTypeInt) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constant.REAL:
                                            switch (Utils.resolveClass(value)) {
                                                case Constant.REAL:
                                                    isMatch = ((DataTypeReal) object).compare((DataTypeReal) value, condition);
                                                    break;

                                                case Constant.DOUBLE:
                                                    isMatch = ((DataTypeReal) object).compare((DataTypeDouble) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constant.DOUBLE:
                                            switch (Utils.resolveClass(value)) {
                                                case Constant.REAL:
                                                    isMatch = ((DataTypeDouble) object).compare((DataTypeReal) value, condition);
                                                    break;

                                                case Constant.DOUBLE:
                                                    isMatch = ((DataTypeDouble) object).compare((DataTypeDouble) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constant.DATE:
                                            isMatch = ((DataTypeDate) object).compare((DataTypeDate) value, condition);
                                            break;

                                        case Constant.DATETIME:
                                            isMatch = ((DataTypeDateTime) object).compare((DataTypeDateTime) value, condition);
                                            break;

                                        case Constant.TEXT:
                                            if(((DataTypeText) object).getValue() != null)
                                                isMatch = ((DataTypeText) object).getValue().equalsIgnoreCase(((DataTypeText) value).getValue());
                                            break;
                                    }
                                    if(!isMatch) break;
                                }
                            }

                            if(isMatch) {
                                AllDataRecords matchedRecord = record;
                                if(selectionColumnIndexList != null) {
                                    matchedRecord = new AllDataRecords();
                                    matchedRecord.setRowIdx(record.getRowIdx());
                                    matchedRecord.setPgLocated(record.getPgLocated());
                                    matchedRecord.setOffset(record.getOffset());
                                    for (Byte index : selectionColumnIndexList) {
                                        matchedRecord.getColumnValueList().add(record.getColumnValueList().get(index));
                                    }
                                }
                                matchRecords.add(matchedRecord);
                                if(getOne) {
                                    randomAccessFile.close();
                                    return matchRecords;
                                }
                            }
                        }
                        if (page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return matchRecords;
                }
            } else {
                ConsoleWriter.displayMessage("Table " + tableName + " does not exist");
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int updateRecord(String databaseName, String tableName, List<Byte> searchColumnIndexList, List<Object> searchValueList, List<Short> searchConditionList, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) {
        List<InternalCondition> conditions = new ArrayList<>();
        for (byte i = 0; i < searchColumnIndexList.size(); i++) {
            conditions.add(InternalCondition.CreateCondition(searchColumnIndexList.get(i), searchConditionList.get(i), searchValueList.get(i)));
        }
        return updateRecord(databaseName, tableName, conditions, updateColumnIndexList, updateColumnValueList, isIncrement);
    }

    public int updateRecord(String databaseName, String tableName, List<InternalCondition> conditions, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) {
        int updateRecordCount = 0;
        try {
            if (conditions == null || updateColumnIndexList == null
                    || updateColumnValueList == null)
                return updateRecordCount;
            if (updateColumnIndexList.size() != updateColumnValueList.size())
                return updateRecordCount;
            File file = new File(databaseName + "/" + tableName + Constant.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                List<AllDataRecords> records = findRecord(databaseName, tableName, conditions, false);
                if (records != null) {
                    if (records.size() > 0) {
                        byte index;
                        Object object;
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                        for (AllDataRecords record : records) {
                            for (int i = 0; i < updateColumnIndexList.size(); i++) {
                                index = updateColumnIndexList.get(i);
                                object = updateColumnValueList.get(i);
                                if (isIncrement) {
                                    record.getColumnValueList().set(index, increment((DataTypeNumeral) record.getColumnValueList().get(index), (DataTypeNumeral) object));
                                } else {
                                    record.getColumnValueList().set(index, object);
                                }
                            }
                            this.writeRecord(randomAccessFile, record);
                            updateRecordCount++;
                        }
                        randomAccessFile.close();
                        return updateRecordCount;
                    }
                }
            } else {
                ConsoleWriter.displayMessage("Table " + tableName + " does not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return updateRecordCount;
    }

    private <T> DataTypeNumeral<T> increment(DataTypeNumeral<T> object1, DataTypeNumeral<T> object2) {
        object1.increment(object2.getValue());
        return object1;
    }

    public Page<AllDataRecords> getLastRecordAndPage(String databaseName, String tableName) {
        try {
            File file = new File(databaseName + "/" + tableName + Constant.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                Page<AllDataRecords> page = getLastPage(file);
                if (page.getNumberOfCells() > 0) {
                    randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + Page.getHeaderFixedLength() + ((page.getNumberOfCells() - 1) * Short.BYTES));
                    short address = randomAccessFile.readShort();
                    AllDataRecords record = getDataRecord(randomAccessFile, page.getPageNumber(), address);
                    if (record != null)
                        page.getPageRecords().add(record);
                }
                randomAccessFile.close();
                return page;
            } else {
                ConsoleWriter.displayMessage("File " + tableName + " does not exist");
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Page getLastPage(File file) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE && page.getRightNodeAddress() != Page.RIGHTMOST_PAGE) {
                page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
            }
            randomAccessFile.close();
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Page getFirstPage(File file) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                if (page.getNumberOfCells() == 0) return null;
                randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + ((short) page.getRecordAddressList().get(0)));
                page = readPageHeader(randomAccessFile, randomAccessFile.readInt());
            }
            randomAccessFile.close();
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public int deleteRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList) {
        return deleteRecord(databaseName, tableName, columnIndexList, valueList, conditionList, true);
    }

    public int deleteRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, boolean deleteOne) {
        int deletedRecordCount = 0;
        try {
            File file = new File(databaseName + "/" + tableName + Constant.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                if(columnIndexList != null) {
                    Page page = getFirstPage(file);
                    AllDataRecords record;
                    boolean isMatch;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Short offset : new ArrayList<Short>(page.getRecordAddressList())) {
                            isMatch = true;
                            record = getDataRecord(randomAccessFile, page.getPageNumber(), offset);
                            for(int i = 0; i < columnIndexList.size(); i++) {
                                isMatch = false;
                                columnIndex = columnIndexList.get(i);
                                value = valueList.get(i);
                                condition = conditionList.get(i);
                                if (record != null && record.getColumnValueList().size() > columnIndex) {
                                    Object object = record.getColumnValueList().get(columnIndex);
                                    switch (Utils.resolveClass(value)) {
                                        case Constant.TINYINT:
                                            isMatch = ((DataTypeTinyInt) value).compare((DataTypeTinyInt) object, condition);
                                            break;

                                        case Constant.SMALLINT:
                                            isMatch = ((DataTypeSmallInt) value).compare((DataTypeSmallInt) object, condition);
                                            break;

                                        case Constant.INT:
                                            isMatch = ((DT_Int) value).compare((DT_Int) object, condition);
                                            break;

                                        case Constant.BIGINT:
                                            isMatch = ((DataTypeInt) value).compare((DataTypeInt) object, condition);
                                            break;

                                        case Constant.REAL:
                                            isMatch = ((DataTypeReal) value).compare((DataTypeReal) object, condition);
                                            break;

                                        case Constant.DOUBLE:
                                            isMatch = ((DataTypeDouble) value).compare((DataTypeDouble) object, condition);
                                            break;

                                        case Constant.DATE:
                                            isMatch = ((DataTypeDate) value).compare((DataTypeDate) object, condition);
                                            break;

                                        case Constant.DATETIME:
                                            isMatch = ((DataTypeDateTime) value).compare((DataTypeDateTime) object, condition);
                                            break;

                                        case Constant.TEXT:
                                            isMatch = ((DataTypeText) value).getValue().equalsIgnoreCase(((DataTypeText) object).getValue());
                                            break;
                                    }
                                    if(!isMatch) break;
                                }
                            }
                            if(isMatch) {
                                page.setNumberOfCells((byte) (page.getNumberOfCells() - 1));
                                page.getRecordAddressList().remove(offset);
                                if(page.getNumberOfCells() == 0) {
                                    page.setStartingAddress((short) (page.getBaseAddress() + Page.PAGE_SIZE - 1));
                                }
                                this.writePageHeader(randomAccessFile, page);
                                UpdateStmtHandler.decrementRowCount(tableName);
                                deletedRecordCount++;
                                if(deleteOne) {
                                    randomAccessFile.close();
                                    return deletedRecordCount;
                                }
                            }
                        }
                        if(page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return deletedRecordCount;
                }
            }
            else {
                ConsoleWriter.displayMessage("Table " + tableName + " does not exist");
                return deletedRecordCount;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return deletedRecordCount;
    }

    public AllDataRecords getDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address) {
        return getDataRecord(randomAccessFile, pageNumber, address, null);
    }

    public AllDataRecords getDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address, List<Byte> columnList) {
        try {
            if (pageNumber >= 0 && address >= 0) {
                AllDataRecords record = new AllDataRecords();
                record.setPgLocated(pageNumber);
                record.setOffset(address);
                randomAccessFile.seek((Page.PAGE_SIZE * pageNumber) + address);
                record.setSize(randomAccessFile.readShort());
                record.setRowIdx(randomAccessFile.readInt());
                byte numberOfColumns = randomAccessFile.readByte();
                byte[] serialTypeCodes = new byte[numberOfColumns];
                for (byte i = 0; i < numberOfColumns; i++) {
                    serialTypeCodes[i] = randomAccessFile.readByte();
                }
                Object object;
                for (byte i = 0; i < numberOfColumns; i++) {
                    switch (serialTypeCodes[i]) {
                       
                        case Constant.ONE_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DataTypeText(null);
                            break;

                        case Constant.TWO_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DataTypeSmallInt(randomAccessFile.readShort(), true);
                            break;

                        case Constant.FOUR_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DataTypeReal(randomAccessFile.readFloat(), true);
                            break;

                        case Constant.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DataTypeDouble(randomAccessFile.readDouble(), true);
                            break;

                        case Constant.TINY_INT_SERIAL_TYPE_CODE:
                            object = new DataTypeTinyInt(randomAccessFile.readByte());
                            break;

                        case Constant.SMALL_INT_SERIAL_TYPE_CODE:
                            object = new DataTypeSmallInt(randomAccessFile.readShort());
                            break;

                        case Constant.INT_SERIAL_TYPE_CODE:
                            object = new DT_Int(randomAccessFile.readInt());
                            break;

                        case Constant.BIG_INT_SERIAL_TYPE_CODE:
                            object = new DataTypeInt(randomAccessFile.readLong());
                            break;

                        case Constant.REAL_SERIAL_TYPE_CODE:
                            object = new DataTypeReal(randomAccessFile.readFloat());
                            break;

                        case Constant.DOUBLE_SERIAL_TYPE_CODE:
                            object = new DataTypeDouble(randomAccessFile.readDouble());
                            break;

                        case Constant.DATE_SERIAL_TYPE_CODE:
                            object = new DataTypeDate(randomAccessFile.readLong());
                            break;

                        case Constant.DATE_TIME_SERIAL_TYPE_CODE:
                            object = new DataTypeDateTime(randomAccessFile.readLong());
                            break;

                        case Constant.TEXT_SERIAL_TYPE_CODE:
                            object = new DataTypeText("");
                            break;

                        default:
                            if (serialTypeCodes[i] > Constant.TEXT_SERIAL_TYPE_CODE) {
                                byte length = (byte) (serialTypeCodes[i] - Constant.TEXT_SERIAL_TYPE_CODE);
                                char[] text = new char[length];
                                for (byte k = 0; k < length; k++) {
                                    text[k] = (char) randomAccessFile.readByte();
                                }
                                object = new DataTypeText(new String(text));
                            } else
                                object = null;
                            break;
                    }
                    if (columnList != null && !columnList.contains(i)) continue;
                    record.getColumnValueList().add(object);
                }
                return record;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    
    public List<String> fetchAllTableColumns(String tableName) {
        List<String> columnNames = new ArrayList<>();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add(CatalogDatabase.COLUMNS_TABLE_SCHEMA_TABLE_NAME);

        List<Object> valueList = new ArrayList<>();
        valueList.add(new DataTypeText(tableName));

        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DataTypeNumeral.EQUALS);

        List<AllDataRecords> records = this.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);

        for (int i = 0; i < records.size(); i++) {
            AllDataRecords record = records.get(i);
            Object object = record.getColumnValueList().get(CatalogDatabase.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnNames.add(((DT) object).getStringValue());
        }

        return columnNames;
    }

    public boolean checkNullConstraint(String tableName, HashMap<String, Integer> columnMap) {

        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add(CatalogDatabase.COLUMNS_TABLE_SCHEMA_TABLE_NAME);

        List<Object> valueList = new ArrayList<>();
        valueList.add(new DataTypeText(tableName));

        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DataTypeNumeral.EQUALS);

        List<AllDataRecords> records = this.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);

        for (int i = 0; i < records.size(); i++) {
            AllDataRecords record = records.get(i);
            Object nullValueObject = record.getColumnValueList().get(CatalogDatabase.COLUMNS_TABLE_SCHEMA_IS_NULLABLE);
            Object object = record.getColumnValueList().get(CatalogDatabase.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);

            String isNullStr = ((DT) nullValueObject).getStringValue();
            boolean isNullable = (isNullStr.compareToIgnoreCase("NULL") == 0) ? false : true;
            if (isNullable) {
                isNullable = (isNullStr.compareToIgnoreCase("NO") == 0) ? true : false;
            }

            if (!columnMap.containsKey(((DT) object).getStringValue()) && isNullable) {
                Utils.printMessage("Field '" + ((DT) object).getStringValue() + "' doesn't have a default value");
                return false;
            }

        }

        return true;
    }

    public HashMap<String, Integer> fetchAllTableColumndataTypes(String tableName) {
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add(CatalogDatabase.COLUMNS_TABLE_SCHEMA_TABLE_NAME);

        List<Object> valueList = new ArrayList<>();
        valueList.add(new DataTypeText(tableName));

        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DataTypeNumeral.EQUALS);

        List<AllDataRecords> records = this.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);
        HashMap<String, Integer> columDataTypeMapping = new HashMap<>();

        for (int i = 0; i < records.size(); i++) {
            AllDataRecords record = records.get(i);
            Object object = record.getColumnValueList().get(CatalogDatabase.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            Object dataTypeObject = record.getColumnValueList().get(CatalogDatabase.COLUMNS_TABLE_SCHEMA_DATA_TYPE);

            String columnName = ((DT) object).getStringValue();
            int columnDataType = Utils.stringToDataType(((DT) dataTypeObject).getStringValue());

            columDataTypeMapping.put(columnName.toLowerCase(), columnDataType);

        }

        return columDataTypeMapping;
    }

    public String getTablePrimaryKey(String tableName) {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(CatalogDatabase.COLUMNS_TABLE_SCHEMA_COLUMN_NAME, InternalCondition.EQUALS, new DataTypeText(tableName)));
        conditions.add(InternalCondition.CreateCondition(CatalogDatabase.COLUMNS_TABLE_SCHEMA_COLUMN_KEY, InternalCondition.EQUALS, new DataTypeText(CatalogDatabase.PRIMARY_KEY_IDENTIFIER)));

        List<AllDataRecords> records = this.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, conditions, false);
        String columnName = "";
        for (AllDataRecords record : records) {
            Object object = record.getColumnValueList().get(CatalogDatabase.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnName = ((DT) object).getStringValue();
            break;
        }

        return columnName;
    }

    public int getTableRecordCount(String tableName) {
        InternalCondition condition = InternalCondition.CreateCondition(CatalogDatabase.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataTypeText(tableName));

        List<AllDataRecords> records = this.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME, condition, true);
        int recordCount = 0;

        for (AllDataRecords record : records) {
            Object object = record.getColumnValueList().get(CatalogDatabase.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            recordCount = Integer.valueOf(((DT) object).getStringValue());
            break;
        }

        return recordCount;
    }

    public boolean checkIfValueForPrimaryKeyExists(String databaseName, String tableName, int value) {
        StorageManager manager = new StorageManager();
        InternalCondition condition = InternalCondition.CreateCondition(0, InternalCondition.EQUALS, new DT_Int(value));

        List<AllDataRecords> records = manager.findRecord(Utils.getUserDatabasePath(databaseName), tableName, condition, false);
        if (records.size() > 0) {
            return true;
        }
        else {
            return false;
        }
    }
}
