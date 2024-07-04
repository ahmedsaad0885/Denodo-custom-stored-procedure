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
                            case "VARCHAR":
                                structValues.add(handleText(jsonParser));
                                break;
                            case "DECIMAL":
                                structValues.add(handleDecimal(jsonParser));
                                break;
                            case "TIMESTAMP":
                                structValues.add(handleTimestamp(jsonParser, formatter));
                                break;
                            case "INTEGER":
                                structValues.add(handleInt(jsonParser));
                                break;
                            case "BIGINT":
                                structValues.add(handleLong(jsonParser));
                                break;
                            case "BOOLEAN":
                                structValues.add(handleBoolean(jsonParser));
                                break;
                            case "ARRAY":
                                if (jsonParser.currentToken() == JsonToken.START_ARRAY) {//recursively calls itself if the struct has an array
                                    List<Struct> nestedStructList = handleStruct(jsonParser, formatter, structMetadata, structNames);
                                    structValues.add(AbstractStoredProcedure.createArray(nestedStructList, Types.STRUCT));
                                }
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




}
