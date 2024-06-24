import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;
import com.denodo.vdb.util.SynchronizeException;
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
                    new StoredProcedureParameter("financial_claim_sub_category_arabic_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT)
                }),
            new StoredProcedureParameter("last_source_update", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
            new StoredProcedureParameter("is_from_ro", Types.BOOLEAN, StoredProcedureParameter.DIRECTION_OUT)
        };
    }
    

    @Override
    
    protected void doCall(Object[] inputValues) throws SynchronizeException, StoredProcedureException {
        String filePath = (String) inputValues[0];
        JsonFactory jsonFactory = new JsonFactory();

        DateTimeFormatter formatter = setTimeFormatter();
        
        Class<?>[] metadata = new Class<?>[] {
            String.class, String.class, BigDecimal.class, String.class, 
            Timestamp.class, Timestamp.class, String.class, String.class
        };
        
        

        try (JsonParser jsonParser = jsonFactory.createParser(new File(filePath))) {
            Integer financialClaimsReleaseOrderId = null;
            Timestamp lastSourceUpdate = null;
            Boolean isFromRo = null;
            List<Struct> financialClaimsList = new ArrayList<>();

            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                        String fieldName = jsonParser.getCurrentName();
                        jsonParser.nextToken();

                        switch (fieldName) {
                        case "financial_claims_release_order_id":
                        	financialClaimsReleaseOrderId = handleInt(jsonParser);break;
                        case "last_source_update":
                        	lastSourceUpdate= handleTimestamp(jsonParser, formatter);break;
                        case "is_from_ro":
                        	isFromRo=handleBoolean(jsonParser);break;
                        case "financial_cliams_list":
                            if (jsonParser.currentToken() == JsonToken.START_ARRAY) {

                            	
                                while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                                    if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                                    	
                                    	
                                    	int stringIndex = 0, decimalIndex = 0, timestampIndex = 0;

                                        String[] stringArray = new String[(int) Arrays.stream(metadata).filter(type -> type == String.class).count()];
                                        BigDecimal[] decimalArray = new BigDecimal[(int) Arrays.stream(metadata).filter(type -> type == BigDecimal.class).count()];
                                        Timestamp[] timestampArray = new Timestamp[(int) Arrays.stream(metadata).filter(type -> type == Timestamp.class).count()];
                                    	
                                        int currentIndex = 0;
                                        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                                            String claimField = jsonParser.getCurrentName();
                                            jsonParser.nextToken();

                                            if (metadata[currentIndex] == String.class) {
                                                stringArray[stringIndex++] = handleText(jsonParser);
                                            } else if (metadata[currentIndex] == BigDecimal.class) {
                                                decimalArray[decimalIndex++] = handleDecimal(jsonParser);
                                            } else if (metadata[currentIndex] == Timestamp.class) {
                                                timestampArray[timestampIndex++] = handleTimestamp(jsonParser, formatter);
                                            }
                                            currentIndex++;
                                        }
                                        List<Object> structValues = new ArrayList<>();
                                        int strIdx = 0, decIdx = 0, tsIdx = 0;
                                        for (Class<?> type : metadata) {
                                            if (type == String.class) {
                                                structValues.add(stringArray[strIdx++]);
                                            } else if (type == BigDecimal.class) {
                                                structValues.add(decimalArray[decIdx++]);
                                            } else if (type == Timestamp.class) {
                                                structValues.add(timestampArray[tsIdx++]);
                                            }
                                        }

                                            Struct struct = super.createStruct(
                                                Arrays.asList(
                                                    "description", "fc_reference_number", "amount_in_riyal", "approval_status_arabic_name",
                                                    "creation_date", "approval_date", "financial_claim_category_arabic_name",
                                                    "financial_claim_sub_category_arabic_name"
                                                ), structValues);
                                            financialClaimsList.add(struct);
                                        }
                                    }
                                }
                                break;
                        }
                    }
                
            Object[] row = new Object[4];
            row[0] = financialClaimsReleaseOrderId;            
            row[1] = createArray(financialClaimsList, Types.STRUCT);
            row[2] = lastSourceUpdate;
            row[3] = isFromRo;
            getProcedureResultSet().addRow(row);
            financialClaimsList.clear();
                }
            }


        } catch (IOException e) {
            throw new StoredProcedureException("Error reading JSON file", e);
        } catch (SQLException e) {
            throw new StoredProcedureException("Error creating SQL array", e);
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
            if (jsonParser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
                return jsonParser.getIntValue() != 0;
            } else if (jsonParser.currentToken() == JsonToken.VALUE_TRUE || jsonParser.currentToken() == JsonToken.VALUE_FALSE) {
                return jsonParser.getBooleanValue();
            }
        } catch (Exception e) {
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
