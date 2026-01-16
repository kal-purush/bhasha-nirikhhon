package jaeseok.numble.mybox.common.redis;

import jaeseok.numble.mybox.common.auth.JwtHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@RequiredArgsConstructor
@Component
public class AuthorizedRequestKeyGenerator implements KeyGenerator {

    private final JwtHandler jwtHandler;

    private final static String DELIMITER = ";";

    @Override
    public Object generate(Object target, Method method, Object... params) {
        StringBuilder keyBuilder = new StringBuilder();

        keyBuilder.append(method.getName());
        keyBuilder.append(DELIMITER);

        for (Object param : params) {
            keyBuilder.append(param);
            keyBuilder.append(DELIMITER);
        }

        keyBuilder.append(jwtHandler.getId());
        keyBuilder.append(DELIMITER);

        System.out.println("key: " + keyBuilder.toString());

        return keyBuilder.toString();
    }
}