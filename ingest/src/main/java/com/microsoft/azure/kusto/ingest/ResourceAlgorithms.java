package com.microsoft.azure.kusto.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.exceptions.ExceptionUtils;
import com.microsoft.azure.kusto.data.instrumentation.FunctionOneException;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.resources.RankedStorageAccount;
import com.microsoft.azure.kusto.ingest.resources.ResourceWithSas;
import com.microsoft.azure.kusto.ingest.utils.SecurityUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ResourceAlgorithms {
    private static final int RETRY_COUNT = 3;
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ResourceAlgorithms() {
    }

    public static <TInner, TWrapper extends ResourceWithSas<TInner>, TOut> Mono<TOut> resourceActionWithRetriesAsync(
            ResourceManager resourceManager,
            List<TWrapper> resources,
            FunctionOneException<Mono<TOut>, TWrapper, Exception> action,
            String actionName,
            Map<String, String> additionalAttributes) {

        if (resources.isEmpty()) {
            throw new IngestionClientException(String.format("%s: No resources were provided.", actionName));
        }

        List<Map<String, String>> totalAttributes = new ArrayList<>();

        return attemptAction(1, resources, resourceManager, action, actionName, additionalAttributes, null, totalAttributes);
    }

    private static <TInner, TWrapper extends ResourceWithSas<TInner>, TOut> Mono<TOut> attemptAction(
            int attempt,
            List<TWrapper> resources,
            ResourceManager resourceManager,
            FunctionOneException<Mono<TOut>, TWrapper, Exception> action,
            String actionName,
            Map<String, String> additionalAttributes,
            Exception ex,
            List<Map<String, String>> totalAttributes) {

        if (attempt > RETRY_COUNT) {
            String errorMessage = String.format("%s: All %d retries failed with last error: %s\n. Used resources: %s",
                    actionName,
                    RETRY_COUNT,
                    ex != null ? ExceptionUtils.getMessageEx(ex) : "",
                    totalAttributes.stream()
                            .map(x -> String.format("%s (%s)", x.get("resource"), x.get("account")))
                            .collect(Collectors.joining(", ")));
            throw new IngestionClientException(errorMessage);
        }

        TWrapper resource = resources.get((attempt - 1) % resources.size());
        Map<String, String> attributes = new HashMap<>();
        attributes.put("resource", resource.getEndpointWithoutSas());
        attributes.put("account", resource.getAccountName());
        attributes.put("type", resource.getClass().getName());
        attributes.put("retry", String.valueOf(attempt));
        attributes.putAll(additionalAttributes);
        totalAttributes.add(attributes);

        return MonitoredActivity.invokeAsync(
                span -> action.apply(resource)
                        .doOnSuccess(ignored -> resourceManager.reportIngestionResult(resource, true)),
                actionName,
                attributes)
                .onErrorResume(e -> {
                    log.warn(String.format("Error during attempt %d of %d for %s.", attempt, RETRY_COUNT, actionName), e);
                    resourceManager.reportIngestionResult(resource, false);
                    return attemptAction(attempt + 1, resources, resourceManager, action, actionName, additionalAttributes, (Exception) e, totalAttributes);
                });
    }

    public static Mono<Void> postToQueueWithRetriesAsync(ResourceManager resourceManager, AzureStorageClient azureStorageClient, IngestionBlobInfo blob) {
        ObjectMapper objectMapper = Utils.getObjectMapper();
        String message;
        try {
            message = objectMapper.writeValueAsString(blob);
        } catch (Exception e) {
            throw new IngestionClientException("Failed to ingest from blob", e);
        }

        return resourceActionWithRetriesAsync(
                resourceManager,
                resourceManager.getShuffledQueues(),
                queue -> azureStorageClient.postMessageToQueue(queue.getAsyncQueue(), message),
                "ResourceAlgorithms.postToQueueWithRetriesAsync",
                Collections.singletonMap("blob", SecurityUtils.removeSecretsFromUrl(blob.getBlobPath())));
    }

    public static Mono<UploadResult> uploadStreamToBlobWithRetriesAsync(ResourceManager resourceManager, AzureStorageClient azureStorageClient,
            InputStream stream,
            String blobName, boolean shouldCompress) {
        return resourceActionWithRetriesAsync(
                resourceManager,
                resourceManager.getShuffledContainers(),
                container -> azureStorageClient.uploadStreamToBlob(stream, blobName, container.getAsyncContainer(), shouldCompress)
                        .map((size) -> {
                            UploadResult uploadResult = new UploadResult();
                            uploadResult.blobPath = container.getAsyncContainer().getBlobContainerUrl() + "/" + blobName + container.getSas();
                            uploadResult.size = size;
                            return uploadResult;
                        }),
                "ResourceAlgorithms.uploadStreamToBlobWithRetriesAsync",
                Collections.emptyMap());
    }

    public static Mono<String> uploadLocalFileWithRetriesAsync(ResourceManager resourceManager, AzureStorageClient azureStorageClient, File file,
            String blobName,
            boolean shouldCompress) {
        return resourceActionWithRetriesAsync(
                resourceManager,
                resourceManager.getShuffledContainers(),
                container -> azureStorageClient.uploadLocalFileToBlob(file, blobName, container.getAsyncContainer(), shouldCompress)
                        .thenReturn(container.getAsyncContainer().getBlobContainerUrl() + "/" + blobName + container.getSas()),
                "ResourceAlgorithms.uploadLocalFileWithRetriesAsync",
                Collections.emptyMap());
    }

    @NotNull
    public static <T> List<T> roundRobinNestedList(@NotNull List<List<T>> validResources) {
        int longestResourceList = validResources.stream().mapToInt(List::size).max().orElse(0);

        // Go from 0 to the longest list length
        return IntStream.range(0, longestResourceList).boxed()
                // This flat maps combines all the inner lists
                .flatMap(i ->
                // For each list, get the i'th element if it exists, or null otherwise (if the list is shorter)
                validResources.stream().map(r -> r.size() > i ? r.get(i) : null)
                        // Remove nulls
                        .filter(Objects::nonNull))
                // So we combine the list of the first element of each list, then the second element, etc.
                .collect(Collectors.toList());
    }

    public static <T extends ResourceWithSas<?>> List<T> getShuffledResources(List<RankedStorageAccount> shuffledAccounts, List<T> resourceOfType) {
        Map<String, List<T>> accountToResourcesMap = groupResourceByAccountName(resourceOfType);

        List<List<T>> validResources = shuffledAccounts.stream()
                // For each shuffled account, get the resources for that account
                .map(account -> accountToResourcesMap.get(account.getAccountName()))
                // Remove nulls and empty lists
                .filter(resourceList -> resourceList != null && !resourceList.isEmpty())
                .collect(Collectors.toList());

        return roundRobinNestedList(validResources);
    }

    private static <T extends ResourceWithSas<?>> Map<String, List<T>> groupResourceByAccountName(List<T> resourceSet) {
        if (resourceSet == null || resourceSet.isEmpty()) {
            return Collections.emptyMap();
        }
        return resourceSet.stream().collect(Collectors.groupingBy(ResourceWithSas::getAccountName, Collectors.toList()));
    }

    public static class UploadResult {
        public String blobPath;
        public int size;
    }
}
