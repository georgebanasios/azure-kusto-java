// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class StreamingIngestClientTest {
    private static StreamingIngestClient streamingIngestClient;
    private IngestionProperties ingestionProperties;

    @Mock
    private static StreamingClient streamingClientMock;

    @Captor
    private static ArgumentCaptor<InputStream> argumentCaptor;

    private final String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";

    @BeforeAll
    static void setUp() {
        streamingClientMock = mock(StreamingClient.class);
        streamingIngestClient = new StreamingIngestClient(streamingClientMock);
        argumentCaptor = ArgumentCaptor.forClass((InputStream.class));
    }

    @BeforeEach
    void setUpEach() throws Exception {
        ingestionProperties = new IngestionProperties("dbName", "tableName");
        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), any(String.class), any(boolean.class))).thenReturn(null);

        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class))).thenReturn(null);
    }

    @Test
    void IngestFromStream_CsvStream() throws Exception {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), isNull(), any(boolean.class));

        /*
         * In order to make efficient ingestion requests, the streaming ingest client compress the given stream unless it is already compressed. When the given
         * stream content is already compressed, the user must specify that in the stream source info. This method verifies if the stream was compressed
         * correctly.
         */
        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void ingestFromStream_CsvStream_WithClientRequestId() throws Exception {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        String clientRequestId = "clientRequestId";
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties, clientRequestId).getIngestionStatusCollection()
                .get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        ArgumentCaptor<ClientRequestProperties> clientRequestPropertiesArgumentCaptor = ArgumentCaptor.forClass(ClientRequestProperties.class);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                clientRequestPropertiesArgumentCaptor.capture(), any(String.class), isNull(), any(boolean.class));

        /*
         * In order to make efficient ingestion requests, the streaming ingest client compress the given stream unless it is already compressed. When the given
         * stream content is already compressed, the user must specify that in the stream source info. This method verifies if the stream was compressed
         * correctly.
         */
        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);

        assertEquals(clientRequestId, clientRequestPropertiesArgumentCaptor.getValue().getClientRequestId());
    }

    @Test
    void ingestFromStream_CompressedCsvStream() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        String data = "Name, Age, Weight, Height";
        byte[] inputArray = StandardCharsets.UTF_8.encode(data).array();
        gzipOutputStream.write(inputArray, 0, inputArray.length);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        // When ingesting compressed data, we should set this property true to avoid double compression.
        streamSourceInfo.setCompressionType(CompressionType.gz);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), isNull(), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void ingestFromStream_JsonStream() throws Exception {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), any(String.class), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void ingestFromStream_CompressedJsonStream() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        byte[] inputArray = StandardCharsets.UTF_8.encode(data).array();
        gzipOutputStream.write(inputArray, 0, inputArray.length);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        streamSourceInfo.setCompressionType(CompressionType.gz);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), any(String.class), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void ingestFromStream_NullStreamSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromStream_NullIngestionProperties_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @ParameterizedTest
    @CsvSource(value = {"null,table", "'',table", "database,null", "database,''"}, nullValues = {"null"})
    void ingestFromBlobStreaming_IngestionPropertiesWithIllegalDatabaseOrTableNames_IllegalArgumentException(String databaseName, String tableName) {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties(databaseName, tableName);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromStream_JsonNoMappingReference_IngestionSucceeds()
            throws IngestionClientException, IngestionServiceException, URISyntaxException {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);
        IngestionResult ingestionResult = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
        assertEquals("Succeeded", ingestionResult.getIngestionStatusCollection().get(0).status.name());
        assertEquals(1, ingestionResult.getIngestionStatusesLength());
    }

    @Test
    void ingestFromStream_JsonWrongMappingKind_IngestionClientException() {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.CSV);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format 'json'; mapping kind should be 'Json', but was 'Csv'."));
    }

    @Test
    void ingestFromStream_AvroNoMappingReference_IngestionSucceeds()
            throws IngestionClientException, IngestionServiceException, URISyntaxException {
        InputStream inputStream = new ByteArrayInputStream(new byte[10]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
        ingestionProperties.setIngestionMapping("AvroMapping", IngestionMapping.IngestionMappingKind.AVRO);
        IngestionResult ingestionResult = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
        assertEquals("Succeeded", ingestionResult.getIngestionStatusCollection().get(0).status.name());
        assertEquals(1, ingestionResult.getIngestionStatusesLength());
    }

    @Test
    void ingestFromStream_AvroWrongMappingKind_IngestionClientException() {
        InputStream inputStream = new ByteArrayInputStream(new byte[10]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.CSV);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format 'avro'; mapping kind should be 'Avro', but was 'Csv'."));
    }

    @Test
    void ingestFromStream_EmptyStream_IngestionClientException() {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Empty stream."));
    }

    @Test
    void ingestFromStream_CaughtDataClientException_IngestionClientException() throws Exception {
        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class))).thenThrow(DataClientException.class);

        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromStream_CaughtDataServiceException_IngestionServiceException() throws Exception {
        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class))).thenThrow(DataServiceException.class);

        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        assertThrows(IngestionServiceException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromFile_Csv() throws Exception {
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class));
    }

    @Test
    void ingestFromFile_Json() throws Exception {
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        String contents = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8).trim();

        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), any(String.class), any(boolean.class));

        verifyCompressedStreamContent(argumentCaptor.getValue(), contents);
    }

    @Test
    void ingestFromFile_CompressedJson() throws Exception {
        String path = resourcesDirectory + "testdata.json.gz";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), any(String.class), any(boolean.class));

        verifyCompressedStreamContent(argumentCaptor.getValue(), jsonDataUncompressed);
    }

    @Test
    void ingestFromFile_NullFileSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromFile_FileSourceInfoWithNullFilePath_IllegalArgumentException() {
        FileSourceInfo fileSourceInfo1 = new FileSourceInfo(null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo1, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromFile_FileSourceInfoWithBlankFilePath_IllegalArgumentException() {
        FileSourceInfo fileSourceInfo2 = new FileSourceInfo("");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo2, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromFile_NullIngestionProperties_IllegalArgumentException() {
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @ParameterizedTest
    @CsvSource(value = {"null,table", "'',table", "database,null", "database,''"}, nullValues = {"null"})
    void ingestFromFile_IngestionPropertiesWithIllegalDatabaseOrTableNames_IllegalArgumentException(String databaseName, String tableName) {
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        ingestionProperties = new IngestionProperties(databaseName, tableName);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromFile_JsonNoMappingReference_IngestionSuccess()
            throws IngestionClientException, IngestionServiceException, URISyntaxException {
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);
        IngestionResult ingestionResult = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties);
        assertEquals("Succeeded", ingestionResult.getIngestionStatusCollection().get(0).status.name());
        assertEquals(1, ingestionResult.getIngestionStatusesLength());
    }

    @Test
    void ingestFromFile_JsonWrongMappingKind_IngestionClientException() {
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.CSV);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format 'json'; mapping kind should be 'Json', but was 'Csv'."));
    }

    @Test
    void ingestFromFile_JsonNoMappingKind_IngestionSuccess() throws IngestionClientException, IngestionServiceException, URISyntaxException {
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        IngestionResult ingestionResult = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties);
        assertEquals("Succeeded", ingestionResult.getIngestionStatusCollection().get(0).status.name());
        assertEquals(1, ingestionResult.getIngestionStatusesLength());
    }

    @Test
    void ingestFromFile_EmptyFile_IngestionClientException() {
        String path = resourcesDirectory + "empty.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Empty file:"));
    }

    @Test
    void ingestFromBlob() throws Exception {
        BlobClient cloudBlockBlob = mock(BlobClient.class);
        String blobPath = "https://kustotest.blob.core.windows.net/container/blob.csv";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath);
        BlobProperties blobProperties = mock(BlobProperties.class);
        when(blobProperties.getBlobSize()).thenReturn((long) 1000);

        BlobInputStream blobInputStream = mock(BlobInputStream.class);
        when(blobInputStream.read(any(byte[].class))).thenReturn(10).thenReturn(-1);

        when(cloudBlockBlob.getProperties()).thenReturn(blobProperties);
        when(cloudBlockBlob.openInputStream()).thenReturn(blobInputStream);

        OperationStatus status = streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties, null).getIngestionStatusCollection()
                .get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class));
    }

    @Test
    void ingestFromBlob_NullBlobSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromBlob_BlobSourceInfoWithNullBlobPath_IllegalArgumentException() {
        BlobSourceInfo blobSourceInfo1 = new BlobSourceInfo(null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo1, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromBlob_BlobSourceInfoWithBlankBlobPath_IllegalArgumentException() {
        BlobSourceInfo blobSourceInfo2 = new BlobSourceInfo("");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo2, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromBlob_NullIngestionProperties_IllegalArgumentException() {
        String path = "blobPath";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(path);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @ParameterizedTest
    @CsvSource(value = {"null,table", "'',table", "database,null", "database,''"}, nullValues = {"null"})
    void ingestFromBlob_IngestionPropertiesWithIllegalDatabaseOrTableNames_IllegalArgumentException(String databaseName, String tableName) {
        String path = "blobPath";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(path);
        ingestionProperties = new IngestionProperties(databaseName, tableName);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromResultSet() throws Exception {
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(1)).thenReturn("Name");
        when(resultSet.getObject(2)).thenReturn("Age");
        when(resultSet.getObject(3)).thenReturn("Weight");

        when(resultSetMetaData.getColumnCount()).thenReturn(3);

        ArgumentCaptor<InputStream> argumentCaptor = ArgumentCaptor.forClass(InputStream.class);

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        OperationStatus status = streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties).getIngestionStatusCollection()
                .get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), isNull(), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, "Name,Age,Weight");
    }

    @Test
    void ingestFromResultSet_NullResultSetSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromResultSet_NullIngestionProperties_IllegalArgumentException() {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @ParameterizedTest
    @CsvSource(value = {"null,table", "'',table", "database,null", "database,''"}, nullValues = {"null"})
    void ingestFromResultSet_IngestionPropertiesWithIllegalDatabaseOrTableNames_IllegalArgumentException(String databaseName, String tableName) {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        ingestionProperties = new IngestionProperties(databaseName, tableName);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void ingestFromResultSet_EmptyResultSet_IngestionClientException() throws Exception {
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(0);

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Empty ResultSet."));
    }

    private static Stream<Arguments> provideStringsForAutoCorrectEndpointTruePass() {
        return Stream.of(
                Arguments.of("https://testendpoint.dev.kusto.windows.net", "https://testendpoint.dev.kusto.windows.net"),
                Arguments.of("https://shouldwork", "https://shouldwork"),
                Arguments.of("https://192.shouldwork.1.1", "https://192.shouldwork.1.1"),
                Arguments.of("https://2345:shouldwork:0425", "https://2345:shouldwork:0425"),
                Arguments.of("https://376.568.1564.1564", "https://376.568.1564.1564"),
                Arguments.of("https://192.168.1.1", "https://192.168.1.1"),
                Arguments.of("https://[2345:0425:2ca1:0000:0000:0567:5673:23b5]", "https://[2345:0425:2ca1:0000:0000:0567:5673:23b5]"),
                Arguments.of("https://127.0.0.1", "https://127.0.0.1"),
                Arguments.of("https://localhost", "https://localhost"),
                Arguments.of("https://onebox.dev.kusto.windows.net", "https://onebox.dev.kusto.windows.net"));
    }

    @ParameterizedTest
    @MethodSource("provideStringsForAutoCorrectEndpointTruePass")
    void autoCorrectEndpoint_True_Pass(String csb, String toCompare) throws URISyntaxException {
        StreamingIngestClient client = IngestClientFactory.createStreamingIngestClient(ConnectionStringBuilder.createWithUserPrompt(csb), null, true);
        assertNotNull(client);
        assertEquals(toCompare, client.connectionDataSource);
    }

    private static Stream<Arguments> provideStringsForAutoCorrectEndpointFalsePass() {
        return Stream.of(
                Arguments.of("https://testendpoint.dev.kusto.windows.net", "https://testendpoint.dev.kusto.windows.net"),
                Arguments.of("https://shouldwork", "https://shouldwork"),
                Arguments.of("https://192.shouldwork.1.1", "https://192.shouldwork.1.1"),
                Arguments.of("https://2345:shouldwork:0425", "https://2345:shouldwork:0425"),
                Arguments.of("https://376.568.1564.1564", "https://376.568.1564.1564"),
                Arguments.of("https://192.168.1.1", "https://192.168.1.1"),
                Arguments.of("https://[2345:0425:2ca1:0000:0000:0567:5673:23b5]", "https://[2345:0425:2ca1:0000:0000:0567:5673:23b5]"),
                Arguments.of("https://127.0.0.1", "https://127.0.0.1"),
                Arguments.of("https://localhost", "https://localhost"),
                Arguments.of("https://onebox.dev.kusto.windows.net", "https://onebox.dev.kusto.windows.net"));
    }

    @ParameterizedTest
    @MethodSource("provideStringsForAutoCorrectEndpointFalsePass")
    void autoCorrectEndpoint_False_Pass(String csb, String toCompare) throws URISyntaxException {
        StreamingIngestClient client = IngestClientFactory.createStreamingIngestClient(ConnectionStringBuilder.createWithUserPrompt(csb), null, false);
        assertNotNull(client);
        assertEquals(toCompare, client.connectionDataSource);
    }

    // Verifies the given stream is compressed correctly and matches the anticipated data content
    public static void verifyCompressedStreamContent(InputStream compressedStream, String data) throws Exception {
        GZIPInputStream gzipInputStream = new GZIPInputStream(compressedStream);
        byte[] buffer = new byte[1];
        byte[] bytes = new byte[4096];
        int index = 0;
        while ((gzipInputStream.read(buffer, 0, 1)) != -1) {
            bytes[index++] = buffer[0];
        }
        String output = new String(bytes).trim();

        assertEquals(data, output);
    }

    public static String jsonDataUncompressed = "{\"Name\":\"demo1\",\"Code\":\"091231\"}\n" +
            "{\"Name\":\"demo11\",\"Code\":\"091232\"}\n" +
            "{\"Name\":\"demo12\",\"Code\":\"091233\"}\n" +
            "{\"Name\":\"demo13\",\"Code\":\"091234\"}\n" +
            "{\"Name\":\"demo14\",\"Code\":\"091235\"}\n" +
            "{\"Name\":\"demo15\",\"Code\":\"091236\"}\n" +
            "{\"Name\":\"demo16\",\"Code\":\"091237\"}\n" +
            "{\"Name\":\"demo17\",\"Code\":\"091238\"}\n" +
            "{\"Name\":\"demo18\",\"Code\":\"091239\"}\n" +
            "{\"Name\":\"demo19\",\"Code\":\"091230\"}\n" +
            "{\"Name\":\"demo10\",\"Code\":\"0912311\"}\n" +
            "{\"Name\":\"demo11\",\"Code\":\"0912322\"}\n" +
            "{\"Name\":\"demo12\",\"Code\":\"0912333\"}\n" +
            "{\"Name\":\"demo13\",\"Code\":\"0912344\"}\n" +
            "{\"Name\":\"demo14\",\"Code\":\"0912355\"}\n" +
            "{\"Name\":\"demo15\",\"Code\":\"0912366\"}\n" +
            "{\"Name\":\"demo16\",\"Code\":\"0912377\"}\n" +
            "{\"Name\":\"demo17\",\"Code\":\"0912388\"}\n" +
            "{\"Name\":\"demo18\",\"Code\":\"0912399\"}\n" +
            "{\"Name\":\"demo19\",\"Code\":\"0912300\"}\n" +
            "{\"Name\":\"demo10\",\"Code\":\"0912113\"}\n" +
            "{\"Name\":\"demo11\",\"Code\":\"0912223\"}\n" +
            "{\"Name\":\"demo12\",\"Code\":\"0912333\"}\n" +
            "{\"Name\":\"demo13\",\"Code\":\"0912443\"}\n" +
            "{\"Name\":\"demo14\",\"Code\":\"0912553\"}\n" +
            "{\"Name\":\"demo15\",\"Code\":\"0912663\"}\n" +
            "{\"Name\":\"demo16\",\"Code\":\"0912773\"}\n" +
            "{\"Name\":\"demo17\",\"Code\":\"0912883\"}\n" +
            "{\"Name\":\"demo18\",\"Code\":\"0912399\"}\n" +
            "{\"Name\":\"demo19\",\"Code\":\"0912003\"}\n" +
            "{\"Name\":\"demo10\",\"Code\":\"091231\"}\n" +
            "{\"Name\":\"demo11\",\"Code\":\"091232\"}\n" +
            "{\"Name\":\"demo12\",\"Code\":\"091233\"}\n" +
            "{\"Name\":\"demo13\",\"Code\":\"091234\"}\n" +
            "{\"Name\":\"demo14\",\"Code\":\"091235\"}";
}
