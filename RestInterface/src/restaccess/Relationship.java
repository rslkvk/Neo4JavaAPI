package restaccess;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

import static restaccess.Request.makeRequest;
import static restaccess.Util.*;

/**
 * Created by resul on 08/04/15.
 */
public class Relationship {

    private static final String NEO_ROOT = Neo4JServerRootURI.NEO4J_ROOT_URI;

    public static Map<String, Object> getRelationship(String id) throws Exception {
        String path = NEO_ROOT+"/relationship/"+id;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> createRelationship(String fromNodeId, String toNodeId,
                                                         String relType) throws Exception {
        String fromPath = NEO_ROOT+"/node/"+fromNodeId+"/relationships";
        String toPath = NEO_ROOT+"/node/"+toNodeId;
        Map<String, Object> objectMap = new HashMap<String, Object>();
        objectMap.put("to",toPath);
        objectMap.put("type",relType);
        String postContent = convertToJson(objectMap);
//      String json = "{\"to\":\""+toPath+"\", \"type\":\""+relType+"\"}";
        Response response = makeRequest(fromPath, postContent, "POST");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static String createRelationship_(String fromNodeId, String toNodeId,
                                                         String relType) throws Exception {
        String fromPath = NEO_ROOT+"/node/"+fromNodeId+"/relationships";
        String toPath = NEO_ROOT+"/node/"+toNodeId;
        Map<String, Object> objectMap = new HashMap<String, Object>();
        objectMap.put("to",toPath);
        objectMap.put("type",relType);
        String postContent = convertToJson(objectMap);
//      String json = "{\"to\":\""+toPath+"\", \"type\":\""+relType+"\"}";
        Response response = makeRequest(fromPath, postContent, "POST");
        validateResponse(response);
        return response.readEntity(String.class);
    }

    public static Map<String, Object> createRelationship(String fromNodeId, String toNodeId,
                                                         String relType, Map<String, Object> property) throws Exception {
        String fromPath = NEO_ROOT+"/node/"+fromNodeId+"/relationships";
        String toPath = NEO_ROOT+"/node/"+toNodeId;
        Map<String, Object> objectMap = new HashMap<String, Object>();
        objectMap.put("to",toPath);
        objectMap.put("type",relType);
        objectMap.put("data",property);
        String json = convertToJson(objectMap);
        Response response = makeRequest(fromPath, json, "POST");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> deleteRelationship(String relationshipId) throws Exception {
        String path = NEO_ROOT+"/relationship/"+relationshipId;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getRelProperties(String relationshipId) throws Exception {
        String path = NEO_ROOT+"/relationship/"+relationshipId+"/properties";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static String getRelationshipProperty(String relationshipId, String propertyKey) throws Exception {
        String path = NEO_ROOT+"/relationship/"+relationshipId+"/properties/"+propertyKey;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return response.readEntity(String.class);
    }

    public static boolean setRelationshipProperty(String relationshipId, String propertyKey,
                                                  String propertyValue) throws Exception {
        String path = NEO_ROOT+"/relationship/"+relationshipId+"/properties/"+propertyKey;
        Response response = makeRequest(path, propertyValue,"PUT");
        validateResponse(response);
        return true;
    }

    public static boolean setRelationshipProperties(String relationshipId, Map<String, Object> properties) throws Exception {
        String path = NEO_ROOT+"/relationship/"+relationshipId+"/properties";
        String putContent = convertToJson(properties);
        Response response = makeRequest(path, putContent,"PUT");
        validateResponse(response);
        return true;
    }

    public static Map<String, Object> getRelationships(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/relationships/all";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getRelationshipsIn(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/relationships/in";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getRelationshipsOut(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/relationships/out";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getRelationshipsByType(String nodeId, String... types) throws Exception {
        String type = types[0];
        for(int i = 1; i <= types.length; i++) {
            type = "&"+types[i];
        }
        String path = NEO_ROOT+"/node/"+nodeId+"/relationships/all/"+type;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getRelationshipType() throws Exception {
        String path = NEO_ROOT+"/relationship/types";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> updateRelationshipProperties(String relationshipId, Map<String, Object>
            properties) throws Exception {
        String path = NEO_ROOT+"relationship/"+relationshipId+"/properties";
        Response response = makeRequest(path, properties, "PUT");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> deleteRelationshipProperties(String relationshipId) throws Exception {
        String path = NEO_ROOT+"relationship/"+relationshipId+"/properties";
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> deleteRelationshipProperty(String relationshipId, String propertyKey)
            throws Exception {
        String path = NEO_ROOT+"relationship/"+relationshipId+"/properties/"+propertyKey;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }
}
