package restaccess;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;

import static restaccess.Request.makeRequest;
import static restaccess.Util.*;

/**
 * Created by resul on 08/04/15.
 */
public class Node {
    private static final String NEO_ROOT = Neo4JServerRootURI.NEO4J_ROOT_URI;

    /**
     * create a single node
     * @return location from the created node
     */
    public static URI createNode() {
        String path = NEO_ROOT+"/node";
        Response r = makeRequest(path, "POST");
        return r.getLocation();
    }

    /**
     * create a node with property
     * @param property key value map with node properties
     * @return location from the created node
     * @throws Exception if request delivered an unvalid response
     */
//    public static URI createNode(Map<String, Object> property) throws Exception {
//        String path = NEO_ROOT+"/node";
//        String jsonProperty = convertToJson(property);
//        Response response = makeRequest(path, jsonProperty, "POST");
//        validateResponse(response);
//        return response.getLocation();
//    }

    /**
     * create node with property
     * @param property json formatted string
     * @return URI from created node
     * @throws Exception if request delivers an unvalid response
     */
    public static URI createNode(String property) throws Exception {
        String path = NEO_ROOT+"/node";
        Response response = makeRequest(path, property, "POST");
        validateResponse(response);
        return response.getLocation();

    }

    //read node
//    public static Map<String, Object> getNode(String nodeId) throws Exception {
//        String path = NEO_ROOT+"/node/"+nodeId;
//        Response response = makeRequest(path, "GET");
//        validateResponse(response);
//        return jsonToMap(response.readEntity(String.class));
//    }

    public static String getNode(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return response.readEntity(String.class);
    }

    //delete node
    public static Map<String, Object> deleteNode(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    //set property
    public static Map<String, Object> setNodeProperty(String nodeId, String propertyKey, String propertyValue) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties/"+propertyKey;
        Response response = makeRequest(path, propertyValue, "PUT");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    //update properties
    public static Map<String, Object> updateNodeProperties(String nodeId, Map<String,Object> properties) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties";
        Response response = makeRequest(path, properties, "PUT");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    //get properties
    public static Map<String, Object> getNodeProperties(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    //get property
    public static String getNodeProperty(String nodeId, String propertyKey) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties/"+propertyKey;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return response.readEntity(String.class);
    }

    //delete all properties from node
    public static Map<String, Object> deleteNodeProperties(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties";
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }
    //delete named property from node
    public static Map<String, Object> deleteNodeProperty(String nodeId, String propertyKey) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties/"+propertyKey;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> addNodeLabel(String nodeId, String nodeLabel) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, nodeLabel, "POST");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> addNodeLabel(String nodeId, String[] nodeLabels) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, nodeLabels, "POST");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> replaceNodeLabels(String nodeId, String[] nodeLabels) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, nodeLabels, "PUT");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> replaceNodeLabels(String nodeId, String nodeLabel) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, nodeLabel, "PUT");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> deleteNodeLabel(String nodeId, String nodeLabel) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels/"+nodeLabel;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getNodeLabels(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getNodesWithLabel(String nodeLabel) throws Exception {
        String path = NEO_ROOT+"/label/"+nodeLabel+"/nodes";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> getNodeWithLabelProperty(String nodeLabel, String nodeKey, String nodeProperty)
            throws Exception {
        String path = NEO_ROOT+"/label/"+nodeLabel+"/nodes?"+nodeKey+"=%22"+nodeProperty+"%22";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static Map<String, Object> listNodeLabels() throws Exception {
        String path = NEO_ROOT+"/labels";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return jsonToMap(response.readEntity(String.class));
    }

    public static int getNodeDegree(String nodeId) {
        return getNodeDegree(nodeId, "all");
    }

    public static int getNodeDegree(String nodeId, String direction) {
        String path = NEO_ROOT+"/node/"+nodeId+"/degree/"+direction;
        Response response = makeRequest(path, "GET");
        return response.readEntity(Integer.class);
    }
}