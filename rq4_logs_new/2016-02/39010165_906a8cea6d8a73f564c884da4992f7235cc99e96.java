package org.dasein.cloud;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Stas Maksimov (stas.maksimov@software.dell.com)
 * @since 0.9.9
 */
public class ResourceNotReadyException extends CloudException {
    
    private int retryAfter;
    private String resourceId;

    /**
     * Operation on the resource <code>resourceId</code> was unsuccessful, no retry suggestions are given
     * @param resourceId resource identifier
     */
    public ResourceNotReadyException(@Nonnull String resourceId) {
        this(resourceId, -1, "Resource not ready");
    }

    /**
     * Operation on the resource <code>resourceId</code> was unsuccessful, no retry suggestions are given
     * @param resourceId resource identifier
     * @param reason additional explanation
     */
    public ResourceNotReadyException(@Nonnull String resourceId, @Nullable String reason) {
        this(resourceId, -1, reason);
    }

    /**
     * Operation on the resource <code>resourceId</code> was unsuccessful, but it may be possible to retry after 
     * a timeout of <code>retryAfter</code> seconds.
     * @param resourceId resource identifier
     * @param retryAfter suggested timeout in seconds
     */
    public ResourceNotReadyException(@Nonnull String resourceId, int retryAfter) {
        this(resourceId, retryAfter, "Resource not ready");
    }

    /**
     * Operation on the resource <code>resourceId</code> was unsuccessful, but it may be possible to retry after 
     * a timeout of <code>retryAfter</code> seconds.
     * @param resourceId resource identifier
     * @param retryAfter suggested timeout in seconds
     * @param reason additional explanation
     */
    public ResourceNotReadyException(@Nonnull String resourceId, int retryAfter, @Nullable String reason) {
        super(reason);
        this.resourceId = resourceId;
        this.retryAfter = retryAfter;
    }

    /**
     * Last operation on the resource may be repeated after a given timeout, unless the timeout value is negative.
     * @return retry timeout in seconds, retry is not recommended if value is negative
     */
    public int getRetryAfter() {
        return retryAfter;
    }

    /**
     * Identifier of the resource upon which the last operation was unsuccessful.
     * @return resource identifier
     */
    public @Nonnull String getResourceId() {
        return resourceId;
    }
}