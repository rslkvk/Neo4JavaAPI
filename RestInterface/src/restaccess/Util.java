package restaccess;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by resul on 08/04/15.
 */
public class Util {

    public static void validateResponse(Response response) throws Exception {
        String responseEntity = response.readEntity(String.class);
        Map<String, Object> responseContent = jsonToMap(responseEntity);
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

    public static boolean isStatusValid(int responseStatus) {
        switch(responseStatus) {
            case 200: return true;
            case 201: return true;
            case 204: return true;
            default: return false;
        }
    }

    public static Map<String, Object> jsonToMap(String json) throws IOException {
        if(!isResponseContentEmpty(json)) {

            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> responseContent = objectMapper.readValue(json,
                    new TypeReference<Map<String, Object>>() {
                    });
            return responseContent;
        }
        return null;
    }

    public static String convertToJson(Object object) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(object);
    }

    public static boolean isResponseContentEmpty(String entity) {
        if(entity.isEmpty() || entity.equals("") || entity == null
                || entity.equals("[]") || entity.equals("{}")) {    //also check for empty json object or json array
            return true;
        }
        return false;
    }

}
