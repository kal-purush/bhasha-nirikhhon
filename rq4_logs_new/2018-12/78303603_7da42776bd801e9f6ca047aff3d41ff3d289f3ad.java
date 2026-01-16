package de._125m125.kt.okhttp.helper;

import java.util.ArrayList;
import java.util.List;

import de._125m125.kt.okhttp.helper.modifier.ClientModifier;
import okhttp3.OkHttpClient;
import okhttp3.Request;

public class OkHttpClientBuilder {
    private final List<ClientModifier> modifiers;
    private OkHttpClient               client;

    public OkHttpClientBuilder(final ClientModifier... modifiers) {
        this.modifiers = new ArrayList<>(modifiers.length);
        for (final ClientModifier clientModifier : modifiers) {
            this.modifiers.add(clientModifier);
        }
    }

    public OkHttpClientBuilder addModifier(final ClientModifier modifier) {
        if (this.client != null) {
            throw new IllegalStateException("The client was already build. No more modifiers can be added.");
        }
        this.modifiers.add(modifier);
        return this;
    }

    public OkHttpClient build() {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
        for (final ClientModifier clientModifier : this.modifiers) {
            clientBuilder = clientModifier.modify(clientBuilder);
        }
        clientBuilder = clientBuilder.addInterceptor(chain -> {
            final Request r = chain.request().newBuilder().removeHeader("userKey").build();
            return chain.proceed(r);
        });
        clientBuilder = clientBuilder.addInterceptor((chain) -> {
            Request request = chain.request();
            if (request.method().equals("GET")) {
                return chain.proceed(request);
            }
            if (request.header("content-type") != null) {
                return chain.proceed(request);
            }
            request = request.newBuilder().addHeader("content-type", "application/x-www-form-urlencoded").build();
            return chain.proceed(request);
        });
        return this.client = clientBuilder.build();
    }
}