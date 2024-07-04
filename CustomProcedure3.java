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
        "INTEGER", "BIGINT", "ARRAY", "ARRAY", "ARRAY", "TIMESTAMP", "BOOLEAN"
    );

    List<List<String>> structMetadata = Arrays.asList(
        Arrays.asList(
            "VARCHAR", "VARCHAR", "DECIMAL", "VARCHAR", "TIMESTAMP", "TIMESTAMP", "VARCHAR", "VARCHAR", "INTEGER"
        ),
        Arrays.asList(
            "INTEGER", "BOOLEAN", "VARCHAR", "VARCHAR"
        ),
        Arrays.asList(
            "VARCHAR", "VARCHAR", "DECIMAL", "VARCHAR"
        )
    );

    List<List<String>> structNames = Arrays.asList(
        Arrays.asList(
            "attr_0", "attr_1", "attr_2", "attr_3", "attr_4", "attr_5", "attr_6", "attr_7", "attr_8"
        ),
        Arrays.asList(
            "attr_a", "attr_b", "attr_c", "attr_d"
        ),
        Arrays.asList(
            "attr_0", "attr_1", "attr_2", "attr_3"
        )
    );

    public String getDescription() {
        return "Get data from JSON file";
    }

    public StoredProcedureParameter[] getParameters() {
        return new StoredProcedureParameter[]{
            new StoredProcedureParameter("filePath", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
            new StoredProcedureParameter("financial_claims_release_order_id", Types.INTEGER, StoredProcedureParameter.DIRECTION_OUT),
            new StoredProcedureParameter("longval", Types.BIGINT, StoredProcedureParameter.DIRECTION_OUT),

            new StoredProcedureParameter("financial_cliams_list", Types.ARRAY, StoredProcedureParameter.DIRECTION_OUT,
                true, new StoredProcedureParameter[]{
                    new StoredProcedureParameter("attr_0", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_1", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_2", Types.DECIMAL, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_3", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_4", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_5", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_6", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_7", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_8", Types.INTEGER, StoredProcedureParameter.DIRECTION_OUT)
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
                    new StoredProcedureParameter("attr_0", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_1", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_2", Types.DECIMAL, StoredProcedureParameter.DIRECTION_OUT),
                    new StoredProcedureParameter("attr_3", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT)
                }),

            new StoredProcedureParameter("last_source_update", Types.TIMESTAMP, StoredProcedureParameter.DIRECTION_OUT),
            new StoredProcedureParameter("is_from_ro", Types.BOOLEAN, StoredProcedureParameter.DIRECTION_OUT)
        };
    }

    @Override
    protected void doCall(Object[] inputValues) throws StoredProcedureException {
        String filePath = (String) inputValues[0];
        JsonFactory jsonFactory = new JsonFactory();
        JsonHandler jsonHandler= new JsonHandler();

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
		// TODO Auto-generated method stub
		return null;
	}
}
