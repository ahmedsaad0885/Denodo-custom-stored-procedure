import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.File;
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

public class CustomProcedure3 extends AbstractStoredProcedure {

    private static final long serialVersionUID = 1L;
    
    List<String> metadata = Arrays.asList(
            "Integer",
            "Struct",
            "Struct",
            "Struct",
            "Timestamp",
            "Boolean"
        );

        List<List<String>> structMetadata = Arrays.asList(
            Arrays.asList("String",
                    "String",
                    "BigDecimal",
                    "String",
                    "Timestamp",
                    "Timestamp",
                    "String",
                    "String",
                    "Integer"
            ),
            Arrays.asList(
            		"Integer",
            	    "Boolean",
            	    "String",
            	    "String"
            ),
            Arrays.asList(
                "String",
                "String",
                "BigDecimal",
                "String"
            )
        );

        List<List<String>> structNames = Arrays.asList(
            Arrays.asList(
                    "description", "fc_reference_number", "amount_in_riyal", "approval_status_arabic_name",
                    "creation_date", "approval_date", "financial_claim_category_arabic_name",
                    "financial_claim_sub_category_arabic_name", "randInt"
            ),
            Arrays.asList(
            		"attr_a", "attr_b", "attr_c", "attr_d"
            ),
            Arrays.asList(

            	    "description", "fc_reference_number", "amount_in_riyal", "approval_status"
            )
        );

        public String getDescription() {
            return "Get data from JSON file";
        }

        public StoredProcedureParameter[] getParameters() {
            return new StoredProcedureParameter[]{
                new StoredProcedureParameter("filePath", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
                new StoredProcedureParameter("financial_claims_release_order_id", Types.INTEGER, StoredProcedureParameter.DIRECTION_OUT),

                new StoredProcedureParameter("financial_cliams_list", Types.ARRAY, StoredProcedureParameter.DIRECTION_OUT,
                    true, new StoredProcedureParameter[]{
                            new StoredProcedureParameter("description", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("fc_reference_number", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("amount_in_riyal", Types.DECIMAL, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("approval_status_arabic_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("creation_date", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("approval_date", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("financial_claim_category_arabic_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("financial_claim_sub_category_arabic_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("randInt", Types.INTEGER, StoredProcedureParameter.DIRECTION_OUT)
                        }),

                new StoredProcedureParameter("financial_cliams_list2", Types.ARRAY, StoredProcedureParameter.DIRECTION_OUT,
                    true, new StoredProcedureParameter[]{
                            new StoredProcedureParameter("attr_a", Types.INTEGER, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("attr_b", Types.BOOLEAN, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("attr_c", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("attr_d", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT)
                        }),

                new StoredProcedureParameter("financial_cliams_list3", Types.ARRAY, StoredProcedureParameter.DIRECTION_OUT,
                    true, new StoredProcedureParameter[]{
                            new StoredProcedureParameter("description", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("fc_reference_number", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("amount_in_riyal", Types.DECIMAL, StoredProcedureParameter.DIRECTION_OUT),
                            new StoredProcedureParameter("approval_status", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT)
                        }),

                new StoredProcedureParameter("last_source_update", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
                new StoredProcedureParameter("is_from_ro", Types.BOOLEAN, StoredProcedureParameter.DIRECTION_OUT)
            };
        }

    @Override
    
    protected void doCall(Object[] inputValues) throws StoredProcedureException {
        String filePath = (String) inputValues[0];
        JsonFactory jsonFactory = new JsonFactory();

        DateTimeFormatter formatter = setTimeFormatter();        
        

        try (JsonParser jsonParser = jsonFactory.createParser(new File(filePath))) {
            

        	List<List<Struct>> structLists = new ArrayList<>();
            for (int i = 0; i < metadata.size(); i++) {
                structLists.add(new ArrayList<>());
            }//TODO change
            
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                    int metadataIndex = 0 , structIndex=0 ;
                    Object[] row = new Object[metadata.size()];

                    while (jsonParser.nextToken() != JsonToken.END_OBJECT && metadataIndex < metadata.size()) {
                        jsonParser.nextToken();

                        String currentType = metadata.get(metadataIndex);
                        switch (currentType) {
                            case "Integer":
                                row[metadataIndex] = handleInt(jsonParser);
                                break;
                            case "Struct":
                                if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
                                	 
                                    List<Struct> structList = handleStruct(jsonParser, formatter, structMetadata.get(structIndex), structNames.get(structIndex));
									row[metadataIndex] = createArray(structList, Types.STRUCT);
									structIndex++;
                                }
                                break;
                            case "Timestamp":
                                row[metadataIndex] = handleTimestamp(jsonParser, formatter);
                                break;
                            case "Boolean":
                                row[metadataIndex] = handleBoolean(jsonParser);
                                break;
                            default:
                                throw new StoredProcedureException("Unsupported type in metadata");
                        }
                        metadataIndex++;
                    }

                    getProcedureResultSet().addRow(row);
                }
            }
        } catch (IOException e) {
            throw new StoredProcedureException("Error reading JSON file", e);
        }
    }

    private String handleText(JsonParser jsonParser) {
        try {
            return jsonParser.getText();
        } catch (Exception e) {
            return null;
        }
    }
    
    private BigDecimal handleDecimal(JsonParser jsonParser) {
        try {
            return jsonParser.getDecimalValue();
        } catch (Exception e) {
            return null;
        }
    }
    
    private Boolean handleBoolean(JsonParser jsonParser) {
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
            e.printStackTrace(); // Print stack trace for debugging
            return null;
        }
        return null;
    }


    private Timestamp handleTimestamp(JsonParser jsonParser, DateTimeFormatter formatter) {
        try {
            return Timestamp.valueOf(LocalDateTime.parse(jsonParser.getText(), formatter));
        } catch (Exception e) {
            return null;
        }
    }
    
    private Integer handleInt(JsonParser jsonParser) {
        try {
            return jsonParser.getIntValue();
        } catch (Exception e) {
            return null;
        }
    }
    
    private List<Struct> handleStruct(JsonParser jsonParser, DateTimeFormatter formatter, List<String> structMetadata, List<String> structNames) {
        List<Struct> structList = new ArrayList<>();
        try {
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                    List<Object> structValues = new ArrayList<>();
                    int currentIndex = 0;
                    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                        jsonParser.nextToken();

                        String type = structMetadata.get(currentIndex);
                        switch (type) {
                            case "String":
                                structValues.add(handleText(jsonParser));
                                break;
                            case "BigDecimal":
                                structValues.add(handleDecimal(jsonParser));
                                break;
                            case "Timestamp":
                                structValues.add(handleTimestamp(jsonParser, formatter));
                                break;
                            case "Integer":
                                structValues.add(handleInt(jsonParser));
                                break;
                            case "Boolean":
                            	structValues.add(handleBoolean(jsonParser));
                            	break;
                            default:
                                throw new StoredProcedureException("Unsupported type in struct metadata");
                        }
                        currentIndex++;
                    }
                    Struct struct = super.createStruct(structNames, structValues);
                    structList.add(struct);
                }
            }
        } catch (IOException | SQLException | StoredProcedureException e) {
            e.printStackTrace();
        }
        return structList;
    }
    
    
    private DateTimeFormatter setTimeFormatter () {
    	return new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .toFormatter();
    	
    }
    
    
    @Override
    public String getName() {
        return "file_reader";
    }

    private Object createArray(List<Struct> elements, int type) throws StoredProcedureException {
        return super.createArray(elements, type);
    }
}