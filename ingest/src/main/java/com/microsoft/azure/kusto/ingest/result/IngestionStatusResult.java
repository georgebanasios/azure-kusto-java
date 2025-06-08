// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;

public class IngestionStatusResult implements IngestionResult {

    private IngestionStatus ingestionStatus;

    public IngestionStatusResult(IngestionStatus ingestionStatus) {
        this.ingestionStatus = ingestionStatus;
    }

    @Override
    public Mono<List<IngestionStatus>> getIngestionStatusCollectionAsync() {
        return Mono.defer(() -> {
            if (ingestionStatus != null) {
                return Mono.just(Collections.singletonList(ingestionStatus));
            }
            return Mono.just(Collections.emptyList());
        });
    }

    @Override
    public List<IngestionStatus> getIngestionStatusCollection() {
        return getIngestionStatusCollectionAsync().block();
    }

    @Override
    public int getIngestionStatusesLength() {
        return 1;
    }
}
