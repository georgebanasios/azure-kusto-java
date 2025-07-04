// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.IClientCertificate;
import com.microsoft.azure.kusto.data.StringUtils;
import com.azure.core.http.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;

public class TokenProviderFactory {
    private TokenProviderFactory() {
        // Hide constructor, since this is a Factory
    }

    public static TokenProviderBase createTokenProvider(@NotNull ConnectionStringBuilder csb, @Nullable HttpClient httpClient) throws URISyntaxException {
        String clusterUrl = csb.getClusterUrl();
        String authorityId = csb.getAuthorityId();
        Mono<String> asyncTokenProvider = csb.getAsyncTokenProvider();

        if (StringUtils.isNotBlank(csb.getApplicationClientId())) {
            if (StringUtils.isNotBlank(csb.getApplicationKey())) {
                return new ApplicationKeyTokenProvider(clusterUrl, csb.getApplicationClientId(), csb.getApplicationKey(), authorityId, httpClient);
            } else if (csb.isUseCertificateAuth()) {
                if (csb.shouldSendX509()) {
                    IClientCertificate clientCertificate = ClientCredentialFactory.createFromCertificateChain(csb.getPrivateKey(),
                            csb.getX509CertificateChain());
                    String applicationClientId = csb.getApplicationClientId();
                    return new SubjectNameIssuerTokenProvider(clusterUrl, applicationClientId, clientCertificate, authorityId, httpClient);
                } else {
                    IClientCertificate clientCertificate = ClientCredentialFactory.createFromCertificate(csb.getPrivateKey(), csb.getX509Certificate());
                    String applicationClientId = csb.getApplicationClientId();
                    return new ApplicationCertificateTokenProvider(clusterUrl, applicationClientId, clientCertificate, authorityId, httpClient);
                }

            } else {
                throw new IllegalArgumentException("No token provider exists for the provided ConnectionStringBuilder");
            }
        } else if (StringUtils.isNotBlank(csb.getAccessToken())) {
            String accessToken = csb.getAccessToken();
            return new AccessTokenTokenProvider(clusterUrl, accessToken);
        } else if (csb.getTokenProvider() != null) {
            return new CallbackTokenProvider(clusterUrl, csb.getTokenProvider());
        } else if (asyncTokenProvider != null) {
            return new AsyncCallbackTokenProvider(clusterUrl, asyncTokenProvider);
        } else if (csb.getCustomTokenCredential() != null) {
            return new TokenCredentialProvider(clusterUrl, csb.getCustomTokenCredential());
        } else if (csb.isUseDeviceCodeAuth()) {
            return new DeviceAuthTokenProvider(clusterUrl, authorityId, httpClient);
        } else if (csb.isUseManagedIdentityAuth()) {
            return new ManagedIdentityTokenProvider(clusterUrl, csb.getManagedIdentityClientId(), httpClient);
        } else if (csb.isUseAzureCli()) {
            return new AzureCliTokenProvider(clusterUrl, httpClient);
        } else if (csb.isUseUserPromptAuth() || csb.isAadFederatedSecurity()) { // If Fed=true, and no other auth method is specified, we assume user prompt
            if (StringUtils.isNotBlank(csb.getUserUsernameHint())) {
                String usernameHint = csb.getUserUsernameHint();
                return new UserPromptTokenProvider(clusterUrl, usernameHint, authorityId, httpClient);
            }
            return new UserPromptTokenProvider(clusterUrl, null, authorityId, httpClient);
        } else {
            return null;
        }
    }
}
