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


public class customized_code_daas_dwh_msql_sql_payment_order extends AbstractStoredProcedure {
	// for handling if the spelling or datatypes change
public static final String typeInt = "INTEGER";
public static final String typeString = "VARCHAR";
public static final String typeDecimal = "DECIMAL";
public static final String typeLong = "BIGINT";
public static final String typeBoolean = "BOOLEAN";
public static final String typeArray = "ARRAY";
public static final String typeTimeStamp = "TIMESTAMP";

    private static final long serialVersionUID = 1L;
    
    
    //metadata start
        
List<String> metadataNames = Arrays.asList(
    "payment_order_id",
    "po_reference_number",
    "agency_code",
    "contract_reference_number",
    "contract_name",
    "creation_date",
    "payment_order_number",
    "ro_reference_number",
    "approval_date",
    "vendor_name",
    "vendor_id",
    "iban",
    "main_vendor_commercial_registration_number",
    "main_vendor_type",
    "bank_name",
    "agency_name",
    "sector_code",
    "sector_name",
    "ministry_name",
    "amount",
    "is_success",
    "approval_status_arabic_name",
    "payment_method_type",
    "payment_method",
    "currency_name",
    "total_amount_in_riyal",
    "contract_id",
    "check_number",
    "check_date",
    "approval_status_english_name",
    "ntis_approval_status",
    "financialclaimslist",
    "list",
    "payment_orders_project_items_list",
    "max_last_src_update",
    "code700",
    "is_sensitive_agency",
    "contract_creation_date",
    "contract_end_date",
    "contract_total_release_amount",
    "contract_vendors_list"
);
List<String> metadataTypes = Arrays.asList(
    "INTEGER",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "TIMESTAMP",
    "VARCHAR",
    "VARCHAR",
    "TIMESTAMP",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "DECIMAL",
    "BOOLEAN",
    "VARCHAR",
    "INTEGER",
    "VARCHAR",
    "VARCHAR",
    "DECIMAL",
    "INTEGER",
    "BIGINT",
    "TIMESTAMP",
    "VARCHAR",
    "VARCHAR",
    "ARRAY",
    "ARRAY",
    "ARRAY",
    "TIMESTAMP",
    "VARCHAR",
    "BOOLEAN",
    "TIMESTAMP",
    "TIMESTAMP",
    "DECIMAL",
    "ARRAY"
);
List<List<String>> structNames = Arrays.asList(
    Arrays.asList(
        "fc_reference_number",
        "creation_date",
        "description",
        "amount_in_riyal",
        "approval_status_arabic_name",
        "approval_date"
    ),
    Arrays.asList(
        "payment_order_number",
        "is_main",
        "payment_method_type",
        "payment_method",
        "vendor_name",
        "vendor_id",
        "commercial_registration_number",
        "iban",
        "amount",
        "amount_for",
        "amount_after_exchange",
        "currency_name",
        "is_success",
        "check_date",
        "check_number",
        "is_deduction",
        "code700"
    ),
    Arrays.asList(
        "project_name",
        "project_number",
        "is_gfs",
        "amount_in_riyal"
    ),
    Arrays.asList(
        "vendor_id",
        "is_main_vendor",
        "vendor_name",
        "commercial_registration_number",
        "vendor_type",
        "is_foreign",
        "nationality_name",
        "code700"
    )
);

List<List<String>> structTypes = Arrays.asList(
    Arrays.asList(
        "VARCHAR",
        "TIMESTAMP",
        "VARCHAR",
        "DECIMAL",
        "VARCHAR",
        "TIMESTAMP"
    ),
    Arrays.asList(
        "VARCHAR",
        "BOOLEAN",
        "INTEGER",
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "DECIMAL",
        "VARCHAR",
        "DECIMAL",
        "VARCHAR",
        "BOOLEAN",
        "TIMESTAMP",
        "BIGINT",
        "INTEGER",
        "VARCHAR"
    ),
    Arrays.asList(
        "VARCHAR",
        "VARCHAR",
        "BOOLEAN",
        "DECIMAL"
    ),
    Arrays.asList(
        "VARCHAR",
        "BOOLEAN",
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "BOOLEAN",
        "VARCHAR",
        "VARCHAR"
    )
);

public StoredProcedureParameter[] getParameters() {

List<String> metadataNames = Arrays.asList(
    "payment_order_id",
    "po_reference_number",
    "agency_code",
    "contract_reference_number",
    "contract_name",
    "creation_date",
    "payment_order_number",
    "ro_reference_number",
    "approval_date",
    "vendor_name",
    "vendor_id",
    "iban",
    "main_vendor_commercial_registration_number",
    "main_vendor_type",
    "bank_name",
    "agency_name",
    "sector_code",
    "sector_name",
    "ministry_name",
    "amount",
    "is_success",
    "approval_status_arabic_name",
    "payment_method_type",
    "payment_method",
    "currency_name",
    "total_amount_in_riyal",
    "contract_id",
    "check_number",
    "check_date",
    "approval_status_english_name",
    "ntis_approval_status",
    "financialclaimslist",
    "list",
    "payment_orders_project_items_list",
    "max_last_src_update",
    "code700",
    "is_sensitive_agency",
    "contract_creation_date",
    "contract_end_date",
    "contract_total_release_amount",
    "contract_vendors_list"
);
List<String> metadataTypes = Arrays.asList(
    "INTEGER",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "TIMESTAMP",
    "VARCHAR",
    "VARCHAR",
    "TIMESTAMP",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "DECIMAL",
    "BOOLEAN",
    "VARCHAR",
    "INTEGER",
    "VARCHAR",
    "VARCHAR",
    "DECIMAL",
    "INTEGER",
    "BIGINT",
    "TIMESTAMP",
    "VARCHAR",
    "VARCHAR",
    "ARRAY",
    "ARRAY",
    "ARRAY",
    "TIMESTAMP",
    "VARCHAR",
    "BOOLEAN",
    "TIMESTAMP",
    "TIMESTAMP",
    "DECIMAL",
    "ARRAY"
);
List<List<String>> structNames = Arrays.asList(
    Arrays.asList(
        "fc_reference_number",
        "creation_date",
        "description",
        "amount_in_riyal",
        "approval_status_arabic_name",
        "approval_date"
    ),
    Arrays.asList(
        "payment_order_number",
        "is_main",
        "payment_method_type",
        "payment_method",
        "vendor_name",
        "vendor_id",
        "commercial_registration_number",
        "iban",
        "amount",
        "amount_for",
        "amount_after_exchange",
        "currency_name",
        "is_success",
        "check_date",
        "check_number",
        "is_deduction",
        "code700"
    ),
    Arrays.asList(
        "project_name",
        "project_number",
        "is_gfs",
        "amount_in_riyal"
    ),
    Arrays.asList(
        "vendor_id",
        "is_main_vendor",
        "vendor_name",
        "commercial_registration_number",
        "vendor_type",
        "is_foreign",
        "nationality_name",
        "code700"
    )
);

List<List<String>> structTypes = Arrays.asList(
    Arrays.asList(
        "VARCHAR",
        "TIMESTAMP",
        "VARCHAR",
        "DECIMAL",
        "VARCHAR",
        "TIMESTAMP"
    ),
    Arrays.asList(
        "VARCHAR",
        "BOOLEAN",
        "INTEGER",
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "DECIMAL",
        "VARCHAR",
        "DECIMAL",
        "VARCHAR",
        "BOOLEAN",
        "TIMESTAMP",
        "BIGINT",
        "INTEGER",
        "VARCHAR"
    ),
    Arrays.asList(
        "VARCHAR",
        "VARCHAR",
        "BOOLEAN",
        "DECIMAL"
    ),
    Arrays.asList(
        "VARCHAR",
        "BOOLEAN",
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "BOOLEAN",
        "VARCHAR",
        "VARCHAR"
    )
);

//get from airflow
List<String> partitions = Arrays.asList("one","two");

//metadata end
    	
    	
        List<StoredProcedureParameter> parameters = new ArrayList<>();
        int structIndex =0;
        
        JsonHandler jsonHandler = new JsonHandler();
        if (metadataTypes.size() != metadataNames.size()) {
            throw new IllegalStateException("Metadata and metadataNames lists must have the same size");
        }
        
        //new
        if (partitions.size() == 1 && partitions.get(0).equals("none")) {
            // No parameters to add
        } else {
            for (int i = 0; i < partitions.size(); i++) {
                String partitionName = "partition" +(i+1);
                parameters.add(new StoredProcedureParameter(partitionName, Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN));
            }
        }

        for (int i = 0; i < metadataTypes.size(); i++) {
            String currentType = metadataTypes.get(i);
            String currentName = metadataNames.get(i);

            if (currentType.equals(typeArray)) {
                List<StoredProcedureParameter> structParameters = new ArrayList<>();
                List<String> currentStructNames = structNames.get(structIndex);
                List<String> currentStructMetadata = structTypes.get(structIndex);

                for (int j = 0; j < currentStructNames.size(); j++) {
                    String structField = currentStructNames.get(j);
                    int sqlType =jsonHandler.getSqlType(currentStructMetadata.get(j));
                    structParameters.add(new StoredProcedureParameter(structField, sqlType, StoredProcedureParameter.DIRECTION_OUT));
                }

                parameters.add(new StoredProcedureParameter(
                    currentName,
                    Types.ARRAY,
                    StoredProcedureParameter.DIRECTION_OUT,
                    true,
                    structParameters.toArray(new StoredProcedureParameter[0])
                ));

                structIndex++;
            } else {
                int sqlType =jsonHandler.getSqlType(currentType);
                parameters.add(new StoredProcedureParameter(currentName, sqlType, StoredProcedureParameter.DIRECTION_OUT));
            }
        }

        return parameters.toArray(new StoredProcedureParameter[0]);
    }


    @Override
    protected void doCall(Object[] inputValues) throws StoredProcedureException {
    	//const
    	String baseFilePath = "D:\\var\\json_files\\";
    	//get from airflow
    	String schema ="daas_dwh_msql";
    	String fileName="sql_payment_order";


        List<String> partitions = new ArrayList<>();
        
        // We assume that the partitions parameters start at a certain index in inputValues. 
        // You need to adjust this index based on your actual setup.
        int partitionStartIndex = 0 /* the index where partition parameters start in inputValues */;
        
        for (int i = partitionStartIndex; i < inputValues.length; i++) {
            if (inputValues[i] != null && !inputValues[i].toString().equalsIgnoreCase("none")) {
                partitions.add(inputValues[i].toString().replaceAll("'", ""));
            }
        }


    	
        StringBuilder filePathBuilder = new StringBuilder();
        filePathBuilder.append(baseFilePath)
                       .append(schema)
                       .append("_")
                       .append(fileName)
                       .append("\\")
                       .append(fileName);
        
        if (!partitions.isEmpty()) {
            for (String partition : partitions) {
                filePathBuilder.append("_").append(partition);
            }
        }
        
        String filePath = filePathBuilder.toString() + ".json";
        
        JsonFactory jsonFactory = new JsonFactory();
        JsonHandler jsonHandler = new JsonHandler();

        DateTimeFormatter formatter = jsonHandler.setTimeFormatter();

        try (JsonParser jsonParser = jsonFactory.createParser(new File(filePath))) {
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                if (jsonParser.currentToken() == JsonToken.START_OBJECT) {// ensures that only JSON objects are processed
                    int metadataIndex = 0, structIndex = 0;
                    Object[] row = new Object[metadataTypes.size()];

                    while (jsonParser.nextToken() != JsonToken.END_OBJECT && metadataIndex < metadataTypes.size()) {
                        jsonParser.nextToken();

                        if (metadataIndex >= metadataTypes.size()) {
                            throw new StoredProcedureException("Metadata index out of bounds");
                        }

                        String currentType = metadataTypes.get(metadataIndex);
                        switch (currentType) {
                            case typeInt:
                                row[metadataIndex] = jsonHandler.handleInt(jsonParser);
                                break;
                            case typeString:
                            	String temp = jsonHandler.handleText(jsonParser);
                            	if (temp =="null" || temp == "") {
                            		temp =null;
                            	}
                                row[metadataIndex] =temp ;
                                break;
                            case typeDecimal:
                                row[metadataIndex] = jsonHandler.handleDecimal(jsonParser);
                                break;
                            case typeLong:
                                row[metadataIndex] = jsonHandler.handleLong(jsonParser);
                                break;
                            case typeBoolean:
                                row[metadataIndex] = jsonHandler.handleBoolean(jsonParser);
                                break;
                            case typeArray:
                                if (jsonParser.currentToken() == JsonToken.START_ARRAY) {//allows the method to identify when it encounters a nested array within a struct and It enables the method to recursively process nested structures,
                                    if (structIndex >= structTypes.size()) {
                                        throw new StoredProcedureException("Struct index out of bounds at Struct");
                                    }
                                    List<Struct> structList = jsonHandler.handleStruct(jsonParser, formatter, structTypes.get(structIndex), structNames.get(structIndex));
                                    row[metadataIndex] = createArray(structList, Types.STRUCT);
                                    structIndex++;
                                }
                                break;
                            case typeTimeStamp:
                                row[metadataIndex] = jsonHandler.handleTimestamp(jsonParser, formatter);
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


    @Override
    public String getName() {
        return "file_reader";
    }
    public String getDescription() {
        return "Get data from JSON file";
    }
}
