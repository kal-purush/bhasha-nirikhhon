/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.estore;

public class Configuration {
    private static final int DEFAULT_MAX_TASK_RETRIES = 3;
    private static final int DEFAULT_MAX_TASK_COUNT = 1024;
    private static final long DEFAULT_RETRY_DELAY_BASEM_S = 5000;

    private Integer  maxTaskCount;
    private Long     maxTaskDataSize;
    private Integer  maxRetryCount;
    private Long     retryDelayBase;

    public Long getMaxTaskDataSize() {
        return maxTaskDataSize == null ? defaultMaxDataSize() : maxTaskDataSize;
    }

    public Integer getMaxTaskCount() {
        return maxTaskCount == null ? DEFAULT_MAX_TASK_COUNT : maxTaskCount;
    }

    public Integer getMaxRetryCount() {
        return maxRetryCount == null ? DEFAULT_MAX_TASK_RETRIES : maxRetryCount;
    }

    public Long getRetryDelayBase() {
        return retryDelayBase == null ? DEFAULT_RETRY_DELAY_BASEM_S : retryDelayBase;
    }

    public Configuration() {
        this(DEFAULT_MAX_TASK_COUNT, DEFAULT_MAX_TASK_RETRIES, DEFAULT_RETRY_DELAY_BASEM_S, defaultMaxDataSize());
    }

    public Configuration(Integer maxTaskCount, Integer maxTaskRetries, Long taskRetryDelayBase, Long maxTaskDataSize) {
        this.maxTaskCount = maxTaskCount;
        this.maxRetryCount = maxTaskRetries;
        this.retryDelayBase = taskRetryDelayBase;
        this.maxTaskDataSize = maxTaskDataSize;

    }

    private static long defaultMaxDataSize() {
        return Runtime.getRuntime().totalMemory()  / 2;
    }

    @Override
    public String toString() {
        String PREFIX = "  \"";
        String SUFFIX = ",\n";
        String SUFFIX_LAST = "\n";
        String SEP = "\" : ";
        return new StringBuilder("{\n")
                .append(PREFIX).append("maxTaskCount").append(SEP).append(getMaxTaskCount()).append(SUFFIX)
                .append(PREFIX).append("maxTaskDataSize").append(SEP).append(getMaxTaskDataSize()).append(SUFFIX)
                .append(PREFIX).append("maxRetryCount").append(SEP).append(getMaxRetryCount()).append(SUFFIX)
                .append(PREFIX).append("retryDelayBase").append(SEP).append(getRetryDelayBase()).append(SUFFIX_LAST)
                .append("}").toString();
    }
}