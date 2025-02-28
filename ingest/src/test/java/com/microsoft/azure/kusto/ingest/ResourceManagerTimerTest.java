package com.microsoft.azure.kusto.ingest;

import static com.microsoft.azure.kusto.ingest.ResourceManagerTest.generateIngestionAuthTokenResult;
import static com.microsoft.azure.kusto.ingest.ResourceManagerTest.generateIngestionResourcesResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import com.microsoft.azure.kusto.data.BaseClient;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;

class ResourceManagerTimerTest {

    @Test
    void timerTest() throws DataClientException, DataServiceException, InterruptedException, KustoServiceQueryError, IOException {
        Client mockedClient = mock(Client.class);
        final List<Date> refreshTimestamps = new ArrayList<>();
        AtomicBoolean gotHere = new AtomicBoolean(false);
        when(mockedClient.executeMgmt(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(generateIngestionAuthTokenResult());
        when(mockedClient.executeMgmt(Commands.INGESTION_RESOURCES_SHOW_COMMAND)).then((Answer<KustoOperationResult>) invocationOnMock -> {
            refreshTimestamps.add((new Date()));
            gotHere.set(true);
            if (refreshTimestamps.size() == 2) {
                throw new Exception();
            }

            return generateIngestionResourcesResult();
        });

        ResourceManager resourceManager = new ResourceManager(mockedClient, 1000L, 500L, null);
        assertTrue(resourceManager.refreshIngestionResourcesTask.refreshedAtLeastOnce.isEmpty());
        assertTrue(resourceManager.refreshIngestionAuthTokenTask.refreshedAtLeastOnce.isEmpty());

        int runtime = 0;
        while (!gotHere.get() && runtime < 5000) {
            Thread.sleep(100);
            runtime += 100;
        }

        assertTrue(resourceManager.refreshIngestionResourcesTask.waitUntilRefreshedAtLeastOnce());
        assertTrue(resourceManager.refreshIngestionAuthTokenTask.waitUntilRefreshedAtLeastOnce());
        assertEquals(1, refreshTimestamps.size());
        Thread.sleep(1100);
        assertEquals(2, refreshTimestamps.size());
        Thread.sleep(600);
        assertEquals(3, refreshTimestamps.size());
        resourceManager.close();
    }

    @Test
    void timerTestFailureGettingResources() throws DataClientException, DataServiceException, InterruptedException {
        Client mockedClient = mock(Client.class);
        final List<Date> refreshTimestamps = new ArrayList<>();
        AtomicBoolean gotHere = new AtomicBoolean(false);
        when(mockedClient.executeMgmt(Commands.IDENTITY_GET_COMMAND))
                .thenThrow(new RuntimeException(BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", null, new Exception(), "error")));
        when(mockedClient.executeMgmt(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenAnswer(invocation -> {
                    refreshTimestamps.add(new Date());
                    gotHere.set(true);
                    throw new RuntimeException(BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", null, new Exception(), "error"));
                });

        ResourceManager resourceManager = new ResourceManager(mockedClient, 1000L, 500L, null);
        int runtime = 0;
        while (!gotHere.get() && runtime < 5000) {
            Thread.sleep(100);
            runtime += 100;
        }

        assertTrue(resourceManager.refreshIngestionResourcesTask.refreshedAtLeastOnce.isEmpty());
        assertTrue(resourceManager.refreshIngestionAuthTokenTask.refreshedAtLeastOnce.isEmpty());
        assertEquals(1, refreshTimestamps.size());
        Thread.sleep(600);
        assertEquals(2, refreshTimestamps.size());
        Thread.sleep(600);
        assertEquals(3, refreshTimestamps.size());
        assertNull(resourceManager.refreshIngestionResourcesTask.waitUntilRefreshedAtLeastOnce(1));
        assertNull(resourceManager.refreshIngestionAuthTokenTask.waitUntilRefreshedAtLeastOnce(1));

        resourceManager.close();
    }
}
