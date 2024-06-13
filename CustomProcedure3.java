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

public class CustomProcedure2 extends AbstractStoredProcedure {

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

        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .toFormatter();

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
                                financialClaimsReleaseOrderId = jsonParser.getIntValue();
                                break;
                            case "last_source_update":
                                lastSourceUpdate = Timestamp.valueOf(LocalDateTime.parse(jsonParser.getText(), formatter));
                                break;
                            case "is_from_ro":
                                if (jsonParser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
                                    isFromRo = jsonParser.getIntValue() != 0;
                                } else if (jsonParser.currentToken() == JsonToken.VALUE_TRUE || jsonParser.currentToken() == JsonToken.VALUE_FALSE) {
                                    isFromRo = jsonParser.getBooleanValue();
                                }
                                break;
                            case "financial_cliams_list":
                                if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
                                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                                        if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                                            String description = null, fcReferenceNumber = null, approvalStatusArabicName = null, financialClaimCategoryArabicName = null, financialClaimSubCategoryArabicName = null;
                                            BigDecimal amountInRiyal = null;
                                            Timestamp creationDate = null, approvalDate = null;

                                            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                                                String claimField = jsonParser.getCurrentName();
                                                jsonParser.nextToken();

                                                switch (claimField) {
                                                    case "attr_0":
                                                        description = jsonParser.getText();
                                                        break;
                                                    case "attr_1":
                                                        fcReferenceNumber = jsonParser.getText();
                                                        break;
                                                    case "attr_2":
                                                        amountInRiyal = jsonParser.getDecimalValue();
                                                        break;
                                                    case "attr_3":
                                                        approvalStatusArabicName = jsonParser.getText();
                                                        break;
                                                    case "attr_4":
                                                        creationDate = Timestamp.valueOf(LocalDateTime.parse(jsonParser.getText(), formatter));
                                                        break;
                                                    case "attr_5":
                                                        approvalDate = Timestamp.valueOf(LocalDateTime.parse(jsonParser.getText(), formatter));
                                                        break;
                                                    case "attr_6":
                                                        financialClaimCategoryArabicName = jsonParser.getText();
                                                        break;
                                                    case "attr_7":
                                                        financialClaimSubCategoryArabicName = jsonParser.getText();
                                                        break;
                                                }
                                            }

                                            List<Object> structValues = Arrays.asList(
                                                description, fcReferenceNumber, amountInRiyal, approvalStatusArabicName, creationDate, approvalDate, financialClaimCategoryArabicName, financialClaimSubCategoryArabicName
                                            );

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



    @Override
    public String getName() {
        return "file_reader";
    }

    private Object createArray(List<Struct> elements, int type) throws StoredProcedureException {
        return super.createArray(elements, type);
    }
}
