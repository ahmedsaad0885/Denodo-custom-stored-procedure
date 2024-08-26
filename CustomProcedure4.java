import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
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

public class CustomProcedure4 extends AbstractStoredProcedure {

    private static final long serialVersionUID = 1L;

    
    
    //metadata start
    List<String> metadata = Arrays.asList(
    "INTEGER", "VARCHAR","ARRAY", "ARRAY","ARRAY","ARRAY"
);

List<String> metadataNames = Arrays.asList(
    "payment_order_id", "po_reference_number","array0", "array1","array1.5","array2"
);


List<List<String>> structNames = Arrays.asList(
		Arrays.asList("id0","id1"),
    Arrays.asList("array_id", "array_text", "inner_array"),
    Arrays.asList("id00","id11"),
    Arrays.asList("array_id2", "array_text2", "inner_array2")
);

 List<List<String>> structMetadata = Arrays.asList(
		 Arrays.asList("INTEGER","INTEGER"),
    Arrays.asList("INTEGER", "VARCHAR", "ARRAY"),
    Arrays.asList("INTEGER","INTEGER"),
    Arrays.asList("INTEGER", "VARCHAR", "ARRAY")

);

 List<List<String>> innerStructNames = Arrays.asList(
 		Arrays.asList("nested_array_id", "nested_array_text"),
 		Arrays.asList("nested_array_id2", "nested_array_text2")

 		);

 		List<List<String>> innerStructMetadata = Arrays.asList(
 		    Arrays.asList("INTEGER", "VARCHAR"),
 		    Arrays.asList("INTEGER", "VARCHAR")

 		);


    
    //metadata end
    public String getDescription() {
        return "Get data from JSON file";
    }

    public StoredProcedureParameter[] getParameters() {
        // Metadata Start
        List<String> metadata = Arrays.asList(
        	    "INTEGER", "VARCHAR","ARRAY", "ARRAY","ARRAY","ARRAY"
        	);

        	List<String> metadataNames = Arrays.asList(
        	    "payment_order_id", "po_reference_number","array0", "array1","array1.5","array2"
        	);


        	List<List<String>> structNames = Arrays.asList(
        			Arrays.asList("id0","id1"),
        	    Arrays.asList("array_id", "array_text", "inner_array"),
        	    Arrays.asList("id00","id11"),
        	    Arrays.asList("array_id2", "array_text2", "inner_array2")
        	);

        	 List<List<String>> structMetadata = Arrays.asList(
        			 Arrays.asList("INTEGER","INTEGER"),
        	    Arrays.asList("INTEGER", "VARCHAR", "ARRAY"),
        	    Arrays.asList("INTEGER","INTEGER"),
        	    Arrays.asList("INTEGER", "VARCHAR", "ARRAY")

        	);
        List<List<String>> innerStructNames = Arrays.asList(
        		Arrays.asList("nested_array_id", "nested_array_text"),
        		Arrays.asList("nested_array_id2", "nested_array_text2")

        		);

        		List<List<String>> innerStructMetadata = Arrays.asList(
        		    Arrays.asList("INTEGER", "VARCHAR"),
        		    Arrays.asList("INTEGER", "VARCHAR")

        		);
        // Metadata End

        List<StoredProcedureParameter> parameters = new ArrayList<>();
        int structIndex = 0;
        int innerStructIndexParam = 0;
        
        JsonHandler jsonHandler = new JsonHandler();
        if (metadata.size() != metadataNames.size()) {
            throw new IllegalStateException("Metadata and metadataNames lists must have the same size");
        }

        parameters.add(new StoredProcedureParameter("filePath", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN));

        for (int i = 0; i < metadata.size(); i++) {
            String currentType = metadata.get(i);
            String currentName = metadataNames.get(i);

            if (currentType.equals("ARRAY")) {
                // Outer Array Handling
                List<StoredProcedureParameter> structParameters = new ArrayList<>();
                List<String> currentStructNames = structNames.get(structIndex);
                List<String> currentStructMetadata = structMetadata.get(structIndex);

                for (int j = 0; j < currentStructNames.size(); j++) {
                    String structField = currentStructNames.get(j);
                    int sqlType = jsonHandler.getSqlType(currentStructMetadata.get(j));

                    if (currentStructMetadata.get(j).equals("ARRAY")) {
                        // Inner Array Handling
                        List<StoredProcedureParameter> innerStructParameters = new ArrayList<>();
                        List<String> innerCurrentStructNames = innerStructNames.get(innerStructIndexParam);
                        List<String> innerCurrentStructMetadata = innerStructMetadata.get(innerStructIndexParam);

                        for (int k = 0; k < innerCurrentStructNames.size(); k++) {
                            String innerStructField = innerCurrentStructNames.get(k);
                            int innerSqlType = jsonHandler.getSqlType(innerCurrentStructMetadata.get(k));

                            innerStructParameters.add(new StoredProcedureParameter(innerStructField, innerSqlType, StoredProcedureParameter.DIRECTION_OUT));
                        }

                        structParameters.add(new StoredProcedureParameter(
                            structField,
                            Types.ARRAY,
                            StoredProcedureParameter.DIRECTION_OUT,
                            true,
                            innerStructParameters.toArray(new StoredProcedureParameter[0])
                        ));
                        innerStructIndexParam++;
                    } else {
                        structParameters.add(new StoredProcedureParameter(structField, sqlType, StoredProcedureParameter.DIRECTION_OUT));
                    }
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
                int sqlType = jsonHandler.getSqlType(currentType);
                parameters.add(new StoredProcedureParameter(currentName, sqlType, StoredProcedureParameter.DIRECTION_OUT));
            }
        }

        return parameters.toArray(new StoredProcedureParameter[0]);
    }


private List<StoredProcedureParameter> innerArrayParam(List<List<String>> innerStructMetadata2, List<List<String>> innerStructNames2, int innerStructIndex, JsonHandler jsonHandler) {
    List<StoredProcedureParameter> structParameters = new ArrayList<>();
    List<String> currentStructNames = innerStructNames2.get(innerStructIndex);
    List<String> currentStructMetadata = innerStructMetadata2.get(innerStructIndex);

    for (int j = 0; j < currentStructNames.size(); j++) {
        String structField = currentStructNames.get(j);
        int sqlType = jsonHandler.getSqlType(currentStructMetadata.get(j));
        structParameters.add(new StoredProcedureParameter(structField, sqlType, StoredProcedureParameter.DIRECTION_OUT));
    }

    return structParameters;
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
                            case "INTEGER":
                                row[metadataIndex] = jsonHandler.handleInt(jsonParser);
                                break;
                            case "BIGINT":
                                row[metadataIndex] = jsonHandler.handleLong(jsonParser);
                                break;
                            case "DECIMAL":
                                row[metadataIndex] = jsonHandler.handleDecimal(jsonParser);
                                break;
                            case "ARRAY":
                                if (jsonParser.currentToken() == JsonToken.START_ARRAY) {//allows the method to identify when it encounters a nested array within a struct and It enables the method to recursively process nested structures,
                                    if (structIndex >= structMetadata.size()) {
                                        throw new StoredProcedureException("Struct index out of bounds at Struct");
                                    }
                                    List<Struct> structList = jsonHandler.handleStruct(jsonParser, formatter, structMetadata.get(structIndex), structNames.get(structIndex));
                                    row[metadataIndex] = createArray(structList, Types.STRUCT);
                                    structIndex++;
                                }
                                break;
                            case "TIMESTAMP":
                                row[metadataIndex] = jsonHandler.handleTimestamp(jsonParser, formatter);
                                break;
                            case "BOOLEAN":
                                row[metadataIndex] = jsonHandler.handleBoolean(jsonParser);
                                break;
                            case "VARCHAR":
                                row[metadataIndex] = jsonHandler.handleText(jsonParser);
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
