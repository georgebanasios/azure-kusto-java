package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCertificate;
import com.microsoft.aad.msal4j.IConfidentialClientApplication;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import com.azure.core.http.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SubjectNameIssuerTokenProvider extends ConfidentialAppTokenProviderBase {
    public static final String SUBJECT_NAME_ISSUER_TOKEN_PROVIDER = "SubjectNameIssuerTokenProvider";
    private final IClientCertificate clientCertificate;

    SubjectNameIssuerTokenProvider(@NotNull String clusterUrl, @NotNull String applicationClientId, @NotNull IClientCertificate clientCertificate,
            String authorityId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, applicationClientId, authorityId, httpClient);
        this.clientCertificate = clientCertificate;
    }

    @Override
    protected IConfidentialClientApplication getClientApplication() throws MalformedURLException {
        ConfidentialClientApplication.Builder builder = ConfidentialClientApplication.builder(applicationClientId, clientCertificate)
                .authority(aadAuthorityUrl)
                .validateAuthority(false)
                .sendX5c(true);
        if (httpClient != null) {
            builder.httpClient(new HttpClientWrapper(httpClient));
        }
        return builder
                .build();
    }

    @Override
    protected String getAuthMethod() {
        return SUBJECT_NAME_ISSUER_TOKEN_PROVIDER;
    }
}
