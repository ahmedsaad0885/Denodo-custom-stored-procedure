import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;
import com.denodo.vdb.util.SynchronizeException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomProcedure extends AbstractStoredProcedure {

    private static final long serialVersionUID = 1L;

    public String getDescription() {
        return "Get data from JSON file";
    }

    public StoredProcedureParameter[] getParameters() {
        return new StoredProcedureParameter[]{
            new StoredProcedureParameter("filePath", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
            new StoredProcedureParameter("name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT),
            new StoredProcedureParameter("age", Types.INTEGER, StoredProcedureParameter.DIRECTION_OUT),
            new StoredProcedureParameter("city", Types.ARRAY, StoredProcedureParameter.DIRECTION_OUT, 
                true, new StoredProcedureParameter[]{
                    new StoredProcedureParameter("cityElement", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT)
                })
        };
    }

    @Override
    protected void doCall(Object[] inputValues) throws SynchronizeException, StoredProcedureException {
        String filePath = (String) inputValues[0];
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            JsonNode rootNode = objectMapper.readTree(new File(filePath));
            for (JsonNode node : rootNode) {
                String name = node.get("name").asText();
                int age = node.get("age").asInt();
                List<String> cities = new ArrayList<>();

                // Iterate over the city array and add its elements to the list
                Iterator<JsonNode> cityIterator = node.get("city").elements();
                while (cityIterator.hasNext()) {
                    cities.add(cityIterator.next().asText());
                }

                Object[] row = new Object[3];
                row[0] = name;
                row[1] = age;
                row[2] = createArray(cities, Types.VARCHAR);

                getProcedureResultSet().addRow(row);
            }
        } catch (IOException e) {
            throw new StoredProcedureException("Error reading JSON file", e);
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
