package dk.kvalitetsit.stakit.service;

import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;

public class BaseUrlProvider {
    private HttpServletRequest request;

    public BaseUrlProvider(HttpServletRequest request) {
        this.request = request;
    }

    public String getBaseUrl() {
        return ServletUriComponentsBuilder.fromRequestUri(request)
                .build()
                .toString();
    }
}