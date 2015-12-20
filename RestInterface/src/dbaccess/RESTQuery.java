package dbaccess;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Resul Kavakli
 * Created by Resul Kavakli on 30/03/15.
 */
public class RESTQuery {
    private static final String NEO_ROOT = "http://localhost:7474/db/data";
    private static final MediaType DEFAULT_MEDIATYPE = MediaType.APPLICATION_JSON_TYPE;

    private static String responseEntity;
    private static Map<String, Object> responseContent;



//    ++++++++++++  NODES  ++++++++++++
    //creat node

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
    public static URI createNode(Map<String, Object> property) throws Exception {
        String path = NEO_ROOT+"/node";
        String jsonProperty = convertToJson(property);
        Response response = makeRequest(path, jsonProperty, "POST");
        validateResponse(response);
        return response.getLocation();

    }

    public static URI createNode(String property) throws Exception {
        String path = NEO_ROOT+"/node";
        Response response = makeRequest(path, property, "POST");
        validateResponse(response);
        return response.getLocation();

    }

    //read node
    public static Map<String, Object> getNode(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    //delete node
    public static Map<String, Object> deleteNode(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
    }

    //set property
    public static Map<String, Object> setNodeProperty(String nodeId, String propertyKey, String propertyValue) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties/"+propertyKey;
        Response response = makeRequest(path, propertyValue, "PUT");
        validateResponse(response);
        return responseContent;
    }

    //update properties
    public static Map<String, Object> updateNodeProperties(String nodeId, Map<String,Object> properties) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties";
        Response response = makeRequest(path, properties, "PUT");
        validateResponse(response);
        return responseContent;
    }

    //get properties
    public static Map<String, Object> getNodeProperties(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    //get property
    public static String getNodeProperty(String nodeId, String propertyKey) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties/"+propertyKey;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseEntity;
    }

    //delete all properties from node
    public static Map<String, Object> deleteNodeProperties(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties";
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
    }
    //delete named property from node
    public static Map<String, Object> deleteNodeProperty(String nodeId, String propertyKey) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/properties/"+propertyKey;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> addNodeLabel(String nodeId, String nodeLabel) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, nodeLabel, "POST");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> addNodeLabel(String nodeId, String[] nodeLabels) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, nodeLabels, "POST");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> replaceNodeLabels(String nodeId, String[] nodeLabels) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, nodeLabels, "PUT");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> replaceNodeLabels(String nodeId, String nodeLabel) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, nodeLabel, "PUT");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> deleteNodeLabel(String nodeId, String nodeLabel) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels/"+nodeLabel;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> getNodeLabels(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/labels";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> getNodesWithLabel(String nodeLabel) throws Exception {
        String path = NEO_ROOT+"/label/"+nodeLabel+"/nodes";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> getNodeWithLabelProperty(String nodeLabel, String nodeKey, String nodeProperty)
                                            throws Exception {
        String path = NEO_ROOT+"/label/"+nodeLabel+"/nodes?"+nodeKey+"=%22"+nodeProperty+"%22";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> listNodeLabels() throws Exception {
        String path = NEO_ROOT+"/labels";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static int getNodeDegree(String nodeId) {
        return getNodeDegree(nodeId, "all");
    }

    public static int getNodeDegree(String nodeId, String direction) {
        String path = NEO_ROOT+"/node/"+nodeId+"/degree/"+direction;
        Response response = makeRequest(path, "GET");
        return response.readEntity(Integer.class);
    }

//    ++++++++++++  Relationships  ++++++++++++

    public static Map<String, Object> getRelationship(String id) throws Exception {
        String path = NEO_ROOT+"/relationship/"+id;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
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
        return responseContent;
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
        return responseContent;
    }

    public static Map<String, Object> deleteRelationship(String relationshipId) throws Exception {
        String path = NEO_ROOT+"/relationship/"+relationshipId;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> getRelProperties(String relationshipId) throws Exception {
        String path = NEO_ROOT+"/relationship/"+relationshipId+"/properties";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static String getRelationshipProperty(String relationshipId, String propertyKey) throws Exception {
        String path = NEO_ROOT+"/relationship/"+relationshipId+"/properties/"+propertyKey;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseEntity;
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
        return responseContent;
    }

    public static Map<String, Object> getRelationshipsIn(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/relationships/in";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> getRelationshipsOut(String nodeId) throws Exception {
        String path = NEO_ROOT+"/node/"+nodeId+"/relationships/out";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> getRelationshipsByType(String nodeId, String... types) throws Exception {
        String type = types[0];
        for(int i = 1; i <= types.length; i++) {
            type = "&"+types[i];
        }
        String path = NEO_ROOT+"/node/"+nodeId+"/relationships/all/"+type;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> getRelationshipType() throws Exception {
        String path = NEO_ROOT+"/relationship/types";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> updateRelationshipProperties(String relationshipId, Map<String, Object>
                                            properties) throws Exception {
        String path = NEO_ROOT+"relationship/"+relationshipId+"/properties";
        Response response = makeRequest(path, properties, "PUT");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> deleteRelationshipProperties(String relationshipId) throws Exception {
        String path = NEO_ROOT+"relationship/"+relationshipId+"/properties";
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> deleteRelationshipProperty(String relationshipId, String propertyKey)
                                            throws Exception {
        String path = NEO_ROOT+"relationship/"+relationshipId+"/properties/"+propertyKey;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
    }


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
        return responseContent;
    }

    public static Map<String, Object> listIndex(String label) throws Exception {
        String path = NEO_ROOT+"/schema/index/"+label;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> deleteIndex(String label, String indexProperty) throws Exception {
        String path = NEO_ROOT+"/schema/index/"+label+"/"+indexProperty;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
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
        return responseContent;
    }

    public static Map<String, Object> getUniqueProperty(String label, String uniqueProperty) throws Exception {
        String path = NEO_ROOT+"/schema/constraint/"+label+"/uniqueness/"+uniqueProperty;
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> getUniqueProperties(String label) throws Exception {
        String path = NEO_ROOT+"/schema/constraint/"+label+"/uniqueness";
        Response response = makeRequest(path, "GET");
        validateResponse(response);
        return responseContent;
    }

    public static Map<String, Object> deleteUniqueProperty(String label, String uniqueProperty) throws Exception {
        String path = NEO_ROOT+"/schema/constraint/"+label+"/uniqueness/"+uniqueProperty;
        Response response = makeRequest(path, "DELETE");
        validateResponse(response);
        return responseContent;
    }

//  ++++++++++++  request methods ++++++++++++
    private static Response makeRequest(String url, String method) {
        return makeRequest(url, DEFAULT_MEDIATYPE, DEFAULT_MEDIATYPE, null, method);
    }

    private static Response makeRequest(String url, String content, String method) {
        return makeRequest(url, DEFAULT_MEDIATYPE, DEFAULT_MEDIATYPE, content, method);
    }

    private static Response makeRequest(String url, Map<String, Object> content, String method) throws JsonProcessingException {
        String contentJson = convertToJson(content);
        return makeRequest(url, DEFAULT_MEDIATYPE, DEFAULT_MEDIATYPE, contentJson, method);
    }

    private static Response makeRequest(String url, String[] content, String method) throws JsonProcessingException {
        String contentJson = convertToJson(content);
        return makeRequest(url, DEFAULT_MEDIATYPE, DEFAULT_MEDIATYPE, contentJson, method);
    }

    private static Response makeRequest(String url, MediaType acceptType, MediaType entityType,
                                        String content, String methodTpe) {
        return createWebTarget(url)
                .accept(acceptType)
                .build(methodTpe, Entity.entity(content, entityType))
                .invoke();
    }

    private static Invocation.Builder createWebTarget(String url) {
        return ClientBuilder.newBuilder().build().target(url).request();
    }
//   ...... end

//    ++++++++++++  private methods ++++++++++++

    private static void validateResponse(Response response) throws Exception {
        responseEntity = response.readEntity(String.class);
        if(isStatusValid(response.getStatus())) {
            if(isResponseContentEmpty(responseEntity)) {
                responseContent = new HashMap<String, Object>();
                responseContent.put("message","the request is successful");
            } else {
                responseContent = jsonToMap(responseEntity);
            }
        } else { //if status code is not valid
            responseContent = jsonToMap(responseEntity);
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("Status code: " + response.getStatus());
            if (responseContent.containsKey("exception")) { //contains the message an exception or only an error message?
                errorMsg.append("\nError message: " + responseContent.get("exception"));
            } else {
                errorMsg.append("\nError message: " + responseContent.get("message"));
            }
            throw new Exception(errorMsg.toString());
        }
    }

    private static boolean isStatusValid(int responseStatus) {
        switch(responseStatus) {
            case 200: return true;
            case 201: return true;
            case 204: return true;
            default: return false;
        }
    }

    private static Map<String, Object> jsonToMap(String json) throws IOException {
        if(!isResponseContentEmpty(json)) {

            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> responseContent = objectMapper.readValue(json,
                    new TypeReference<Map<String, Object>>() {
                    });
            return responseContent;
        }
        return null;
    }

    private static String convertToJson(Object object) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(object);
    }

    private static boolean isResponseContentEmpty(String entity) {
        if(entity.isEmpty() || entity.equals("") || entity == null
                || entity.equals("[]") || entity.equals("{}")) {    //also check for empty json object or json array
            return true;
        }
        return false;
    }
}