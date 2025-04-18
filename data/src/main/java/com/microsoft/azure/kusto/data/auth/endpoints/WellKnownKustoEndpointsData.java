package com.microsoft.azure.kusto.data.auth.endpoints;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

public class WellKnownKustoEndpointsData {
    public static class AllowedEndpoints {
        public ArrayList<String> AllowedKustoSuffixes;
        public ArrayList<String> AllowedKustoHostnames;
    }

    @JsonIgnore
    public ArrayList<String> _Comments;
    public HashMap<String, AllowedEndpoints> AllowedEndpointsByLogin;
    private static WellKnownKustoEndpointsData instance = null;
    private static final Object object = new Object();

    public static WellKnownKustoEndpointsData getInstance() {
        if (instance != null)
            return instance;
        synchronized (object) {
            if (instance == null) {
                instance = readInstance();
            }
        }
        return instance;
    }

    // For Deserialization
    public WellKnownKustoEndpointsData() {
    }

    @NotNull
    private static WellKnownKustoEndpointsData readInstance() {
        try {
            // Beautiful !
            ObjectMapper objectMapper = Utils.getObjectMapper();
            try (InputStream resourceAsStream = WellKnownKustoEndpointsData.class.getResourceAsStream(
                    "/WellKnownKustoEndpoints.json")) {
                return objectMapper.readValue(resourceAsStream, WellKnownKustoEndpointsData.class);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to read WellKnownKustoEndpoints.json", ex);
        }
    }
}
