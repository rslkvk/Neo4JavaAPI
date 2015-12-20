package restaccess;

import com.fasterxml.jackson.core.JsonProcessingException;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Created by resul on 08/04/15.
 */
public class Request {

    private static MediaType DEFAULT_MEDIATYPE = MediaType.APPLICATION_JSON_TYPE;

    public static void setDefaultMediatype(MediaType defaultMediatype) {
        DEFAULT_MEDIATYPE = defaultMediatype;
    }

    public static MediaType getDefaultMediatype() {
        return DEFAULT_MEDIATYPE;
    }

    public static Response makeRequest(String url, String method) {
        return makeRequest(url, DEFAULT_MEDIATYPE, DEFAULT_MEDIATYPE, null, method);
    }

    public static Response makeRequest(String url, String content, String method) {
        return makeRequest(url, DEFAULT_MEDIATYPE, DEFAULT_MEDIATYPE, content, method);
    }

    public static Response makeRequest(String url, Map<String, Object> content, String method) throws JsonProcessingException {
        String contentJson = Util.convertToJson(content);
        return makeRequest(url, DEFAULT_MEDIATYPE, DEFAULT_MEDIATYPE, contentJson, method);
    }

    public static Response makeRequest(String url, String[] content, String method) throws JsonProcessingException {
        String contentJson = Util.convertToJson(content);
        return makeRequest(url, DEFAULT_MEDIATYPE, DEFAULT_MEDIATYPE, contentJson, method);
    }

    private static Response makeRequest(String url, MediaType acceptType, MediaType entityType,
                                        String content, String methodType) {
        return createWebTarget(url)
                .accept(acceptType)
                .build(methodType, Entity.entity(content, entityType))
                .invoke();
    }

    private static Invocation.Builder createWebTarget(String url) {
        return ClientBuilder.newBuilder().build().target(url).request();
    }
}
