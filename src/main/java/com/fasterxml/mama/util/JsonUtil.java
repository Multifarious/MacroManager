package com.fasterxml.mama.util;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtil
{
    private final static ObjectMapper MAPPER = new ObjectMapper();

    public static String asJSONString(Object value) throws IOException {
        return MAPPER.writeValueAsString(value);
    }

    public static byte[] asJSONBytes(Object value) throws IOException {
        return MAPPER.writeValueAsBytes(value);
    }

    public static <T> T fromJSON(String json, Class<T> cls) throws IOException {
        return MAPPER.readValue(json, cls);
    }

    public static <T> T fromJSON(byte[] json, Class<T> cls) throws IOException {
        return MAPPER.readValue(json, cls);
    }

    public static ObjectNode fromJSON(byte[] json) throws IOException {
        return (ObjectNode) MAPPER.readTree(json);
    }
    
    public static ObjectNode objectNode() {
        return MAPPER.createObjectNode();
    }
}
