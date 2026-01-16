package com.modsen.ridesservice.client.driver;

import com.modsen.ridesservice.client.CustomFeignClientException;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerErrorException;

@Component
public class DriverFeignClientFallbackFactory implements FallbackFactory<DriverFeignClient> {

    @Override
    public DriverFeignClient create(Throwable cause) {
        return (driverId, acceptLanguage) -> {
            if (cause.getCause() instanceof CustomFeignClientException customFeignClientException) {
                throw customFeignClientException;
            }
            throw new ServerErrorException(cause.getMessage(), cause);
        };
    }

}