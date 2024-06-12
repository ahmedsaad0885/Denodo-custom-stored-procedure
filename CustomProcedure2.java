import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;
import com.denodo.vdb.util.SynchronizeException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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
            new StoredProcedureParameter("last_source_update", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
            new StoredProcedureParameter("is_from_ro", Types.BOOLEAN, StoredProcedureParameter.DIRECTION_OUT),
            new StoredProcedureParameter("financial_claims_list", Types.ARRAY, StoredProcedureParameter.DIRECTION_OUT,
                true, new StoredProcedureParameter[]{
                    new StoredProcedureParameter("description", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("fc_reference_number", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("amount_in_riyal", Types.DECIMAL, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("approval_status_arabic_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("creation_date", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("approval_date", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("financial_claim_category_arabic_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("financial_claim_sub_category_arabic_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT)
                })
        };
    }


    @Override
    protected void doCall(Object[] inputValues) throws SynchronizeException, StoredProcedureException {
        String filePath = (String) inputValues[0];
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            JsonNode rootNode = objectMapper.readTree(new File(filePath));
            List<Struct> financialClaimsList = new ArrayList<>();
            Integer financialClaimsReleaseOrderId = null;
            Timestamp lastSourceUpdate = null;
            Boolean isFromRo = null;

            for (JsonNode node : rootNode) {
                String description = node.get("description").asText();
                String fcReferenceNumber = node.get("fc_reference_number").asText();
                BigDecimal amountInRiyal = node.get("amount_in_riyal").decimalValue();
                String approvalStatusArabicName = node.get("approval_status_arabic_name").asText();
                Timestamp creationDate = Timestamp.valueOf(node.get("creation_date").asText());
                Timestamp approvalDate = Timestamp.valueOf(node.get("approval_date").asText());
                String financialClaimCategoryArabicName = node.get("financial_claim_category_arabic_name").asText();
                String financialClaimSubCategoryArabicName = node.get("financial_claim_sub_category_arabic_name").asText();

                // Only set financialClaimsReleaseOrderId, lastSourceUpdate, and isFromRo once as they are not part of the array
                if (financialClaimsReleaseOrderId == null && lastSourceUpdate == null && isFromRo == null) {
                    financialClaimsReleaseOrderId = node.get("financial_claims_release_order_id").asInt();
                    lastSourceUpdate = Timestamp.valueOf(node.get("last_source_update").asText());
                    isFromRo = node.get("is_from_ro").asBoolean();
                }

                List<Object> structValues = Arrays.asList(
                    description, 
                    fcReferenceNumber, 
                    amountInRiyal, 
                    approvalStatusArabicName, 
                    creationDate, 
                    approvalDate, 
                    financialClaimCategoryArabicName, 
                    financialClaimSubCategoryArabicName
                );

                Struct struct = super.createStruct(
                    Arrays.asList(
                        "description", "fc_reference_number", "amount_in_riyal", "approval_status_arabic_name",
                        "creation_date", "approval_date", "financial_claim_category_arabic_name",
                        "financial_claim_sub_category_arabic_name"
                    ), structValues);
                financialClaimsList.add(struct);
            }

            Object[] row = new Object[4];
            row[0] = financialClaimsReleaseOrderId;
            row[1] = lastSourceUpdate;
            row[2] = isFromRo;
            row[3] = createArray(financialClaimsList, Types.STRUCT);
            getProcedureResultSet().addRow(row);
        } catch (IOException e) {
            throw new StoredProcedureException("Error reading JSON file", e);
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public String getName() {
        return "file_reader";
    }

    private Object createArray(List<String> elements, int type) throws StoredProcedureException {
        return super.createArray(elements, type);
    }
}
