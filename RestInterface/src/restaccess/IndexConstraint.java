package restaccess;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static restaccess.Request.makeRequest;
import static restaccess.Util.*;

/**
 * Created by resul on 08/04/15.
 */
public class IndexConstraint {
    private static final String NEO_ROOT = Neo4JServerRootURI.NEO4J_ROOT_URI;

    //index and constraints
    public static Map<String, Object> createIndex(String label, String indexProperty) throws Exception {
        String path = NEO_ROOT+"/schema/index/"+label;
        List<String> list = new ArrayList<String>();
        list.add(indexProperty);
        Map<String, Object> objectMap = new HashMap<String, Object>();
        objectMap.put("property_keys",list);
        String content = convertToJson(objectMap);
        Response response = makeRequest(path, content, "POST");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> listIndex(String label) throws Exception {
        String path = NEO_ROOT+"/schema/index/"+label;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> deleteIndex(String label, String indexProperty) throws Exception {
        String path = NEO_ROOT+"/schema/index/"+label+"/"+indexProperty;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> setUniqueProperty(String label, String uniqueProperty) throws Exception {
        String path = NEO_ROOT+"/schema/constraint/"+label+"/uniqueness";
        List<String> list = new ArrayList<String>();
        list.add(uniqueProperty);
        Map<String, Object> objectMap = new HashMap<String, Object>();
        objectMap.put("property_keys",list);
        String content = convertToJson(objectMap);
        Response response = makeRequest(path, content, "POST");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getUniqueProperty(String label, String uniqueProperty) throws Exception {
        String path = NEO_ROOT+"/schema/constraint/"+label+"/uniqueness/"+uniqueProperty;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getUniqueProperties(String label) throws Exception {
        String path = NEO_ROOT+"/schema/constraint/"+label+"/uniqueness";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> deleteUniqueProperty(String label, String uniqueProperty) throws Exception {
        String path = NEO_ROOT+"/schema/constraint/"+label+"/uniqueness/"+uniqueProperty;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }
}
