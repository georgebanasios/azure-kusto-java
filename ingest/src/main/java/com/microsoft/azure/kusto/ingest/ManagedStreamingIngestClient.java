package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.exceptions.ExceptionUtils;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.microsoft.azure.kusto.data.exceptions.OneApiError;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.*;
import com.microsoft.azure.kusto.data.ExponentialRetry;

import com.microsoft.azure.kusto.ingest.utils.IngestionUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.UUID;

/**
 * <p>ManagedStreamingIngestClient</p>
 * <p>
 * This class combines a managed streaming client with a queued streaming client, to create an optimized experience.
 * Since the streaming client communicates directly with the engine, it's more prone to failure, so this class
 * holds both a streaming client and a queued client.
 * It tries {@value ATTEMPT_COUNT} times using the streaming client, after which it falls back to the queued streaming client in case of failure.
 * By default the policy for choosing a queued ingestion on the first try is the checking of weather the size of the estimated
 * raw stream size (a conversion to compressed CSV) is bigger than 4MB, it will fall back to the queued streaming client.
 * Use {@link #setQueuingPolicyFactor(double)} to override the predicate heuristics.
 * Use SourceInfo.setRawSizeInBytes to set the raw size of the data.
 */
public class ManagedStreamingIngestClient extends IngestClientBase implements QueuedIngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final int ATTEMPT_COUNT = 3;
    public static final String CLASS_NAME = ManagedStreamingIngestClient.class.getSimpleName();
    final QueuedIngestClient queuedIngestClient;
    final StreamingIngestClient streamingIngestClient;
    private final ExponentialRetry exponentialRetryTemplate;
    private HttpClient httpClient = null;
    private ManagedStreamingQueuingPolicy queuingPolicy = ManagedStreamingQueuingPolicy.Default;
    private static final String fallbackLogString = "Data size for source id '%s' is greater than max streaming size according to the policy. Falling back to queued.";

    /**
     * @param dmConnectionString dm connection string
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from a DM connection string, with default http client properties.
     * This method infers the engine connection string from the DM connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromDmConnectionString(ConnectionStringBuilder dmConnectionString) throws URISyntaxException {
        return fromDmConnectionString(dmConnectionString, (HttpClientProperties) null);
    }

    /**
     * @param dmConnectionString dm connection string
     * @param properties         additional properties to configure the http client
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from a DM connection string.
     * This method infers the engine connection string from the DM connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromDmConnectionString(ConnectionStringBuilder dmConnectionString,
            @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        ConnectionStringBuilder engineConnectionString = new ConnectionStringBuilder(dmConnectionString);
        engineConnectionString.setClusterUrl(IngestClientBase.getQueryEndpoint(engineConnectionString.getClusterUrl()));
        return new ManagedStreamingIngestClient(dmConnectionString, engineConnectionString, properties);
    }

    /**
     * @param engineConnectionString engine connection string
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from an engine connection string, with default http client properties.
     * This method infers the DM connection string from the engine connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromEngineConnectionString(ConnectionStringBuilder engineConnectionString) throws URISyntaxException {
        return fromEngineConnectionString(engineConnectionString, null);
    }

    /**
     * @param engineConnectionString engine connection string
     * @param properties             additional properties to configure the http client
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from an engine connection string.
     * This method infers the DM connection string from the engine connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromEngineConnectionString(ConnectionStringBuilder engineConnectionString,
            @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        ConnectionStringBuilder dmConnectionString = new ConnectionStringBuilder(engineConnectionString);
        dmConnectionString.setClusterUrl(IngestClientBase.getIngestionEndpoint(engineConnectionString.getClusterUrl()));
        return new ManagedStreamingIngestClient(dmConnectionString, engineConnectionString, properties);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - This method is slated to be private. Use
     * {@link IngestClientFactory#createManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     * instead.
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
            ConnectionStringBuilder queryEndpointConnectionStringBuilder) throws URISyntaxException {
        this(ingestionEndpointConnectionStringBuilder, queryEndpointConnectionStringBuilder, null);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @param autoCorrectEndpoint                      - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
            ConnectionStringBuilder queryEndpointConnectionStringBuilder, boolean autoCorrectEndpoint) throws URISyntaxException {
        this(ingestionEndpointConnectionStringBuilder, queryEndpointConnectionStringBuilder, null, autoCorrectEndpoint);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @param properties                               - Additional properties to configure the http client
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - This method is slated to be private.  Use
     * {@link IngestClientFactory#createManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder, HttpClientProperties)} instead.
     * This constructor should only be used for advanced cases. If your endpoints are standard, or you do not know, use
     * {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)})} instead.
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
            ConnectionStringBuilder queryEndpointConnectionStringBuilder,
            @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(ingestionEndpointConnectionStringBuilder, properties, autoCorrectEndpoint);
        streamingIngestClient = new StreamingIngestClient(queryEndpointConnectionStringBuilder, properties, autoCorrectEndpoint);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param connectionStringBuilder - Client connection string
     * @param properties              - Additional properties to configure the http client
     * @param autoCorrectEndpoint     - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
            @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, properties, autoCorrectEndpoint);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, properties, autoCorrectEndpoint);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param connectionStringBuilder - Client connection string
     * @param httpClient              - HTTP client
     * @param autoCorrectEndpoint     - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
            @Nullable HttpClient httpClient, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, httpClient, autoCorrectEndpoint);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, httpClient, autoCorrectEndpoint);
        this.httpClient = httpClient;
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @param properties                               - Additional properties to configure the http client
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
            ConnectionStringBuilder queryEndpointConnectionStringBuilder,
            @Nullable HttpClientProperties properties) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(ingestionEndpointConnectionStringBuilder, properties, true);
        streamingIngestClient = new StreamingIngestClient(queryEndpointConnectionStringBuilder, properties, true);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /***
     * @param connectionStringBuilder - Client connection string
     * @param properties - Additional properties to configure the http client
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
            @Nullable HttpClientProperties properties) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, properties, true);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, properties, true);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /***
     * @param connectionStringBuilder - Client connection string
     * @param httpClient - HTTP client
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
            @Nullable HttpClient httpClient) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, httpClient, true);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, httpClient, true);
        this.httpClient = httpClient;
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param resourceManager ingestion resources manager
     * @param storageClient   - storage utilities
     * @param streamingClient - the streaming client
     * @deprecated - This method is slated to be private. Use
     * {@link IngestClientFactory#createManagedStreamingIngestClient(ConnectionStringBuilder)} instead.
     */
    public ManagedStreamingIngestClient(ResourceManager resourceManager,
            AzureStorageClient storageClient,
            StreamingClient streamingClient) {
        log.info("Creating a new ManagedStreamingIngestClient from raw parts");
        queuedIngestClient = new QueuedIngestClientImpl(resourceManager, storageClient);
        streamingIngestClient = new StreamingIngestClient(streamingClient);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param resourceManager ingestion resources manager
     * @param storageClient   - storage utilities
     * @param streamingClient - the streaming client
     * @param retryTemplate   - retry template
     */
    public ManagedStreamingIngestClient(ResourceManager resourceManager,
            AzureStorageClient storageClient,
            StreamingClient streamingClient,
            ExponentialRetry retryTemplate) {
        log.info("Creating a new ManagedStreamingIngestClient from raw parts");
        queuedIngestClient = new QueuedIngestClientImpl(resourceManager, storageClient);
        streamingIngestClient = new StreamingIngestClient(streamingClient);
        exponentialRetryTemplate = retryTemplate;
    }

    ManagedStreamingIngestClient(StreamingIngestClient streamingIngestClient, QueuedIngestClient queuedIngestClient, ExponentialRetry exponentialRetry) {
        this.streamingIngestClient = streamingIngestClient;
        this.queuedIngestClient = queuedIngestClient;
        exponentialRetryTemplate = exponentialRetry;
    }

    @Override
    protected IngestionResult ingestFromFileImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        fileSourceInfo.validate();
        ingestionProperties.validate();
        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.fileToStream(fileSourceInfo, true);
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (FileNotFoundException e) {
            log.error("File not found when ingesting a file.", e);
            throw new IngestionClientException("IO exception - check file path.", e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     */
    @Override
    protected IngestionResult ingestFromBlobImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();

        BlobClientBuilder blobClientBuilder = new BlobClientBuilder().endpoint(blobSourceInfo.getBlobPath());
        if (httpClient != null) {
            blobClientBuilder.httpClient(httpClient);
        }

        BlobClient blobClient = blobClientBuilder.buildClient();
        long blobSize = 0;
        if (blobSourceInfo.getBlobExactSize() != null) {
            try {
                blobSize = blobClient.getProperties().getBlobSize();
            } catch (BlobStorageException e) {
                throw new IngestionServiceException(
                        blobSourceInfo.getBlobPath(),
                        "Failed getting blob properties: " + ExceptionUtils.getMessageEx(e),
                        e);
            }
        }

        if (queuingPolicy.shouldUseQueuedIngestion(blobSize,
                blobSourceInfo.getCompressionType() != null, ingestionProperties.getDataFormat())) {
            log.info(String.format(fallbackLogString, blobSourceInfo.getSourceId()));
            return queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        }

        IngestionResult result = streamWithRetries(blobSourceInfo, ingestionProperties, blobClient);
        if (result != null) {
            return result;
        }
        return queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
    }

    private IngestionResult streamWithRetries(SourceInfo sourceInfo, IngestionProperties ingestionProperties, @Nullable BlobClient blobClient)
            throws IngestionClientException, IngestionServiceException {
        ExponentialRetry<IngestionClientException, IngestionServiceException> retry = new ExponentialRetry<>(
                exponentialRetryTemplate);
        return retry.execute(currentAttempt -> {
            try {
                if (blobClient != null) {
                    String clientRequestId = String.format("KJC.executeManagedStreamingIngest.ingestFromBlob;%s;%d", sourceInfo.getSourceId(), currentAttempt);
                    return streamingIngestClient.ingestFromBlob((BlobSourceInfo) sourceInfo, ingestionProperties, clientRequestId);
                } else {
                    String clientRequestId = String.format("KJC.executeManagedStreamingIngest.ingestFromStream;%s;%d", sourceInfo.getSourceId(),
                            currentAttempt);
                    return streamingIngestClient.ingestFromStream((StreamSourceInfo) sourceInfo, ingestionProperties, clientRequestId);
                }
            } catch (Exception e) {
                if (e instanceof IngestionServiceException
                        && e.getCause() != null
                        && e.getCause() instanceof DataServiceException
                        && e.getCause().getCause() != null
                        && e.getCause().getCause() instanceof DataWebException) {
                    DataWebException webException = (DataWebException) e.getCause().getCause();
                    OneApiError oneApiError = webException.getApiError();
                    if (oneApiError.isPermanent()) {
                        throw e;
                    }
                }
                log.info(String.format("Streaming ingestion failed attempt %d", currentAttempt), e);

                if (sourceInfo instanceof StreamSourceInfo) {
                    try {
                        ((StreamSourceInfo) sourceInfo).getStream().reset();
                    } catch (IOException ioException) {
                        throw new IngestionClientException("Failed to reset stream", ioException);
                    }
                }

            }
            return null;
        });
    }

    @Override
    protected IngestionResult ingestFromResultSetImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        resultSetSourceInfo.validate();
        ingestionProperties.validateResultSetProperties();
        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.resultSetToStream(resultSetSourceInfo);
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (IOException ex) {
            String msg = "Failed to read from ResultSet.";
            log.error(msg, ex);
            throw new IngestionClientException(msg, ex);
        }
    }

    @Override
    protected IngestionResult ingestFromStreamImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException, IOException {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        streamSourceInfo.validate();
        ingestionProperties.validate();

        UUID sourceId = streamSourceInfo.getSourceId();
        if (sourceId == null) {
            sourceId = UUID.randomUUID();
        }

        streamSourceInfo.setSourceId(sourceId);
        byte[] streamingBytes;
        InputStream byteArrayStream;

        if (queuingPolicy.shouldUseQueuedIngestion(streamSourceInfo.getStream().available(),
                streamSourceInfo.getCompressionType() != null, ingestionProperties.getDataFormat())) {
            log.info(String.format(fallbackLogString, sourceId));
            return queuedIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
        }

        try {
            if (streamSourceInfo.getStream() instanceof ByteArrayInputStream || streamSourceInfo.getStream() instanceof ResettableFileInputStream) {
                byteArrayStream = streamSourceInfo.getStream();
            } else {
                // If its not a ByteArrayInputStream:
                // Read 10mb (max streaming size), decide with that if we should stream
                streamingBytes = IngestionUtils.readBytesFromInputStream(streamSourceInfo.getStream(),
                        ManagedStreamingQueuingPolicy.MAX_STREAMING_STREAM_SIZE_BYTES + 1);
                byteArrayStream = new ByteArrayInputStream(streamingBytes);
                int size = streamingBytes.length;
                if (queuingPolicy.shouldUseQueuedIngestion(size,
                        streamSourceInfo.getCompressionType() != null, ingestionProperties.getDataFormat())) {
                    StreamSourceInfo managedSourceInfo = new StreamSourceInfo(new SequenceInputStream(byteArrayStream, streamSourceInfo.getStream()),
                            streamSourceInfo.isLeaveOpen(), sourceId, streamSourceInfo.getCompressionType());
                    log.info(String.format(fallbackLogString, managedSourceInfo.getSourceId()));

                    return queuedIngestClient.ingestFromStream(managedSourceInfo, ingestionProperties);
                }

                if (!streamSourceInfo.isLeaveOpen()) {
                    // From this point we don't need the original stream anymore, we cached it
                    try {
                        streamSourceInfo.getStream().close();
                    } catch (IOException e) {
                        log.warn("Failed to close stream", e);
                    }
                }
            }
        } catch (IOException e) {
            throw new IngestionClientException("Failed to read from stream.", e);
        }

        StreamSourceInfo managedSourceInfo = new StreamSourceInfo(byteArrayStream, true, sourceId, streamSourceInfo.getCompressionType());
        try {
            IngestionResult result = streamWithRetries(managedSourceInfo, ingestionProperties, null);
            if (result != null) {
                return result;
            }

            return queuedIngestClient.ingestFromStream(managedSourceInfo, ingestionProperties);
        } finally {
            try {
                managedSourceInfo.getStream().close();
            } catch (IOException e) {
                log.warn("Failed to close byte stream", e);
            }
        }
    }

    /**
     * <p>setQueuingPolicyFactor</p>
     * A factor used to tune the policy that handles the logic over which data size would the client choose to directly
     * use queued ingestion instead of trying streaming ingestion first.
     * Setting the factor will create a new {@link ManagedStreamingQueuingPolicy} with this factor, which will be used
     * in the future ingestion calls.
     * @param factor - Default is 1.
     **/
    public void setQueuingPolicyFactor(double factor) {
        this.queuingPolicy = new ManagedStreamingQueuingPolicy(factor);
    }

    @Override
    protected String getClientType() {
        return CLASS_NAME;
    }

    @Override
    public void close() throws IOException {
        queuedIngestClient.close();
        streamingIngestClient.close();
    }

    @Override
    public void setQueueRequestOptions(RequestRetryOptions queueRequestOptions) {
        queuedIngestClient.setQueueRequestOptions(queueRequestOptions);
    }

    @Override
    public IngestionResourceManager getResourceManager() {
        return queuedIngestClient.getResourceManager();
    }
}
