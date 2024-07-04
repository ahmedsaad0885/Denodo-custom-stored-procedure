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
        List<String> metadata = Arrays.asList(
        "INTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "TIMESTAMP", "VARCHAR",
        "VARCHAR", "TIMESTAMP", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR",
        "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "DECIMAL",
        "BOOLEAN", "VARCHAR", "INTEGER", "VARCHAR", "VARCHAR", "DECIMAL", "INTEGER",
        "VARCHAR", "TIMESTAMP", "VARCHAR", "VARCHAR", "ARRAY", "ARRAY", "ARRAY",
        "TIMESTAMP", "VARCHAR", "BOOLEAN", "TIMESTAMP", "TIMESTAMP", "DECIMAL",
        "ARRAY"
    );

    List<String> metadataNames = Arrays.asList(
        "Integer", "String1", "String2", "String3", "String4", "Timestamp1", "String5",
        "String6", "Timestamp2", "String7", "String8", "String9", "String10", "String11",
        "String12", "String13", "String14", "String15", "String16", "String17", "BigDecimal1",
        "Boolean1", "String18", "Integer2", "String19", "String20", "BigDecimal2", "Integer3",
        "String21", "Timestamp3", "String22", "String23", "Struct1", "Struct2", "Struct3",
        "Timestamp4", "String24", "Boolean2", "Timestamp5", "Timestamp6", "BigDecimal3",
        "Struct4"
    );

    List<List<String>> structMetadata = Arrays.asList(
        Arrays.asList("VARCHAR", "TIMESTAMP", "VARCHAR", "DECIMAL", "VARCHAR", "TIMESTAMP"),
        Arrays.asList("VARCHAR", "BOOLEAN", "INTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "DECIMAL", "VARCHAR", "DECIMAL", "VARCHAR", "BOOLEAN", "TIMESTAMP", "BIGINT", "INTEGER", "VARCHAR"),
        Arrays.asList("VARCHAR", "VARCHAR", "BOOLEAN", "DECIMAL"),
        Arrays.asList("VARCHAR", "BOOLEAN", "VARCHAR", "VARCHAR", "VARCHAR", "BOOLEAN", "VARCHAR", "VARCHAR")
    );

    List<List<String>> structNames = Arrays.asList(
        Arrays.asList("fc_reference_number", "creation_date", "description", "amount_in_riyal", "approval_status_arabic_name", "approval_date"),
        Arrays.asList("payment_order_number", "is_main", "payment_method_type", "payment_method", "vendor_name", "vendor_id", "commercial_registration_number", "iban", "amount", "amount_for", "amount_after_exchange", "currency_name", "is_success", "check_date", "check_number", "is_deduction", "code700"),
        Arrays.asList("project_name", "project_number", "is_gfs", "amount_in_riyal"),
        Arrays.asList("vendor_id", "is_main_vendor", "vendor_name", "commercial_registration_number", "vendor_type", "is_foreign", "nationality_name", "code700")
    );
    
    //metadata end
    public String getDescription() {
        return "Get data from JSON file";
    }

    public StoredProcedureParameter[] getParameters() {
    	
    	//metadata start
        List<String> metadata = Arrays.asList(
                "INTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "TIMESTAMP", "VARCHAR",
                "VARCHAR", "TIMESTAMP", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR",
                "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "DECIMAL",
                "BOOLEAN", "VARCHAR", "INTEGER", "VARCHAR", "VARCHAR", "DECIMAL", "INTEGER",
                "VARCHAR", "TIMESTAMP", "VARCHAR", "VARCHAR", "ARRAY", "ARRAY", "ARRAY",
                "TIMESTAMP", "VARCHAR", "BOOLEAN", "TIMESTAMP", "TIMESTAMP", "DECIMAL",
                "ARRAY"
            );

            List<String> metadataNames = Arrays.asList(
                "Integer", "String1", "String2", "String3", "String4", "Timestamp1", "String5",
                "String6", "Timestamp2", "String7", "String8", "String9", "String10", "String11",
                "String12", "String13", "String14", "String15", "String16", "String17", "BigDecimal1",
                "Boolean1", "String18", "Integer2", "String19", "String20", "BigDecimal2", "Integer3",
                "String21", "Timestamp3", "String22", "String23", "Struct1", "Struct2", "Struct3",
                "Timestamp4", "String24", "Boolean2", "Timestamp5", "Timestamp6", "BigDecimal3",
                "Struct4"
            );

            List<List<String>> structMetadata = Arrays.asList(
                Arrays.asList("VARCHAR", "TIMESTAMP", "VARCHAR", "DECIMAL", "VARCHAR", "TIMESTAMP"),
                Arrays.asList("VARCHAR", "BOOLEAN", "INTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "DECIMAL", "VARCHAR", "DECIMAL", "VARCHAR", "BOOLEAN", "TIMESTAMP", "BIGINT", "INTEGER", "VARCHAR"),
                Arrays.asList("VARCHAR", "VARCHAR", "BOOLEAN", "DECIMAL"),
                Arrays.asList("VARCHAR", "BOOLEAN", "VARCHAR", "VARCHAR", "VARCHAR", "BOOLEAN", "VARCHAR", "VARCHAR")
            );

            List<List<String>> structNames = Arrays.asList(
                Arrays.asList("fc_reference_number", "creation_date", "description", "amount_in_riyal", "approval_status_arabic_name", "approval_date"),
                Arrays.asList("payment_order_number", "is_main", "payment_method_type", "payment_method", "vendor_name", "vendor_id", "commercial_registration_number", "iban", "amount", "amount_for", "amount_after_exchange", "currency_name", "is_success", "check_date", "check_number", "is_deduction", "code700"),
                Arrays.asList("project_name", "project_number", "is_gfs", "amount_in_riyal"),
                Arrays.asList("vendor_id", "is_main_vendor", "vendor_name", "commercial_registration_number", "vendor_type", "is_foreign", "nationality_name", "code700")
            );
//metadata end
    	
    	
        List<StoredProcedureParameter> parameters = new ArrayList<>();
        int structIndex =0;
        
        JsonHandler jsonHandler = new JsonHandler();
        if (metadata.size() != metadataNames.size()) {
            throw new IllegalStateException("Metadata and metadataNames lists must have the same size");
        }


            parameters.add(new StoredProcedureParameter("filePath", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN));

        for (int i = 0; i < metadata.size(); i++) {
            String currentType = metadata.get(i);
            String currentName = metadataNames.get(i);

            if (currentType.equals(typeArray)) {
                List<StoredProcedureParameter> structParameters = new ArrayList<>();
                List<String> currentStructNames = structNames.get(structIndex);
                List<String> currentStructMetadata = structMetadata.get(structIndex);

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
        String filePath = (String) inputValues[0];
        JsonFactory jsonFactory = new JsonFactory();
        JsonHandler jsonHandler = new JsonHandler();

        DateTimeFormatter formatter = jsonHandler.setTimeFormatter();

        try (JsonParser jsonParser = jsonFactory.createParser(new File(filePath))) {
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                if (jsonParser.currentToken() == JsonToken.START_OBJECT) {// ensures that only JSON objects are processed
                    int metadataIndex = 0, structIndex = 0;
                    Object[] row = new Object[metadata.size()];

                    while (jsonParser.nextToken() != JsonToken.END_OBJECT && metadataIndex < metadata.size()) {
                        jsonParser.nextToken();

                        if (metadataIndex >= metadata.size()) {
                            throw new StoredProcedureException("Metadata index out of bounds");
                        }

                        String currentType = metadata.get(metadataIndex);
                        switch (currentType) {
                            case typeInt:
                                row[metadataIndex] = jsonHandler.handleInt(jsonParser);
                                break;
                            case typeString:
                                row[metadataIndex] = jsonHandler.handleText(jsonParser);
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
                                    if (structIndex >= structMetadata.size()) {
                                        throw new StoredProcedureException("Struct index out of bounds at Struct");
                                    }
                                    List<Struct> structList = jsonHandler.handleStruct(jsonParser, formatter, structMetadata.get(structIndex), structNames.get(structIndex));
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
}
