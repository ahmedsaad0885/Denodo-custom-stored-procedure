import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JsonHandler {
	
	// for handling if the spelling or datatypes change
public static final String typeInt = "INTEGER";
public static final String typeString = "VARCHAR";
public static final String typeDecimal = "DECIMAL";
public static final String typeLong = "BIGINT";
public static final String typeBoolean = "BOOLEAN";
public static final String typeArray = "ARRAY";
public static final String typeTimeStamp = "TIMESTAMP";
    public List<Struct> handleStruct(JsonParser jsonParser, DateTimeFormatter formatter, List<String> structMetadata,
    		List<String> structNames) throws StoredProcedureException {
        List<Struct> structList = new ArrayList<>();
        try {
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                if (jsonParser.currentToken() == JsonToken.START_OBJECT) {// ensures that only JSON objects are processed
                    List<Object> structValues = new ArrayList<>();
                    int currentIndex = 0;
                    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                        jsonParser.nextToken();

                        if (currentIndex >= structMetadata.size()) {
                            throw new StoredProcedureException("Struct metadata index out of bounds");
                        }

                        String type = structMetadata.get(currentIndex);
                        switch (type) {
                        	case typeInt:
                            structValues.add(handleInt(jsonParser));
                            break;
                            case typeString:
                                structValues.add(handleText(jsonParser));
                                break;
                            case typeDecimal:
                                structValues.add(handleDecimal(jsonParser));
                                break;
                            case typeLong:
                                structValues.add(handleLong(jsonParser));
                                break;
                            case typeBoolean:
                                structValues.add(handleBoolean(jsonParser));
                                break;
                            case typeArray:
                                if (jsonParser.currentToken() == JsonToken.START_ARRAY) {//recursively calls itself if the struct has an array
                                    List<Struct> nestedStructList = handleStruct(jsonParser, formatter, structMetadata, structNames);
                                    structValues.add(AbstractStoredProcedure.createArray(nestedStructList, Types.STRUCT));
                                }
                                break;
                            case typeTimeStamp:
                                structValues.add(handleTimestamp(jsonParser, formatter));
                                break;
                            default:
                                throw new StoredProcedureException("Unsupported type in struct metadata");
                        }
                        currentIndex++;
                    }
                    Struct struct = AbstractStoredProcedure.createStruct(structNames, structValues);
                    structList.add(struct);
                }
            }
        } catch (IOException | SQLException e) {
            throw new StoredProcedureException("Error processing struct", e);
        }
        return structList;
    }

    private Object createArray(List<Struct> elements, int type) throws StoredProcedureException {
        return AbstractStoredProcedure.createArray(elements, type);
    }

    private Object createArray(Object[] elements, int type) throws StoredProcedureException {
        List<Object> elementList = Arrays.asList(elements);
        return AbstractStoredProcedure.createArray(elementList, type);
    }

    public String handleText(JsonParser jsonParser) {
        try {
            return jsonParser.getText();
        } catch (Exception e) {
            return null;
        }
    }
    
    public BigDecimal handleDecimal(JsonParser jsonParser) {
        try {
            return jsonParser.getDecimalValue();
        } catch (Exception e) {
            return null;
        }
    }
    
    public Boolean handleBoolean(JsonParser jsonParser) {
        try {
            JsonToken currentToken = jsonParser.currentToken();
            if (currentToken == JsonToken.VALUE_NUMBER_INT) {
                return jsonParser.getIntValue() != 0;
            } else if (currentToken == JsonToken.VALUE_TRUE) {
                return true;
            } else if (currentToken == JsonToken.VALUE_FALSE) {
                return false;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    public Timestamp handleTimestamp(JsonParser jsonParser, DateTimeFormatter formatter) {
        try {
            return Timestamp.valueOf(LocalDateTime.parse(jsonParser.getText(), formatter));
        } catch (Exception e) {
            return null;
        }
    }
    
    public Integer handleInt(JsonParser jsonParser) {
        try {
            return jsonParser.getIntValue();
        } catch (Exception e) {
            return null;
        }
    }
    
    public Long handleLong(JsonParser jsonParser) {
        try {
            return jsonParser.getLongValue();
        } catch (Exception e) {
            return null;
        }
    }

    public DateTimeFormatter setTimeFormatter() {
        return new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();
    }
    public int getSqlType(String type) {
        switch (type) {
            case typeString:
                return Types.VARCHAR;
            case typeInt:
                return Types.INTEGER;
            case typeDecimal:
                return Types.DECIMAL;
            case typeLong:
                return Types.BIGINT;
            case typeBoolean:
                return Types.BOOLEAN;
            case typeArray:
            	return Types.ARRAY;
            case typeTimeStamp:
                return Types.TIMESTAMP;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }



}
