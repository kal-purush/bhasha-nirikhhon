package org.simpleRpcFamework.core.basevo;

import lombok.Data;

/**
 * Created by luo on 02/05/2017.
 */
@Data
public class Request {
    private long requestId;
    private Class<?> clazz;
    private String method;
    private Class<?>[] parameterTypes;
    private Object[] params;
    private long requestTime;
}