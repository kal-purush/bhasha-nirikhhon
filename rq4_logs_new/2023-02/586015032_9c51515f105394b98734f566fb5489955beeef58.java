/*
 * Copyright 2023 Ilya Shishkov (https://github.com/shishkovilja)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.ignitop.ignite.system;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 */
public class SystemMetricsInformation {
    /** Consistent ID. */
    private final Object consistentId;

    /** Host names. */
    private final Collection<String> hostNames;

    /** CPU load in percents. */
    private final double cpuLoadPercent;

    /** Load average. */
    private final double loadAverage;

    /** Garbage collector CPU load in percents. */
    private final double gcCpuLoadPercent;

    /** Heap usage in percents. */
    private final double heapUsagePercent;

    /** Data regions usages in percents (sorted by data region name). */
    private final SortedMap<String, Double> dataRegionUsagesPercents;

    /** Data storage size in gigabytes. */
    private final double dataStorageSizeGigabytes;

    /**
     * @param consistentId             Consistent ID.
     * @param hostNames                Node host names.
     * @param cpuLoadPercent           CPU load in percents.
     * @param loadAverage              Load average.
     * @param gcCpuLoadPercent         GC CPU load in percents.
     * @param heapUsagePercent         Heap used.
     * @param dataRegionUsagesPercents Data region usages in percents.
     * @param dataStorageSizeGigabytes Data storage size in GB.
     */
    public SystemMetricsInformation(
        Object consistentId,
        Collection<String> hostNames,
        double cpuLoadPercent,
        double loadAverage,
        double gcCpuLoadPercent,
        double heapUsagePercent,
        Map<String, Double> dataRegionUsagesPercents,
        double dataStorageSizeGigabytes)
    {
        this.consistentId = consistentId;
        this.hostNames = Collections.unmodifiableCollection(hostNames);
        this.cpuLoadPercent = cpuLoadPercent;
        this.loadAverage = loadAverage;
        this.gcCpuLoadPercent = gcCpuLoadPercent;
        this.heapUsagePercent = heapUsagePercent;

        this.dataRegionUsagesPercents = new TreeMap<>(String::compareTo);
        this.dataRegionUsagesPercents.putAll(dataRegionUsagesPercents);

        this.dataStorageSizeGigabytes = dataStorageSizeGigabytes;
    }

    /**
     * @return Consistent ID.
     */
    public Object consistentId() {
        return consistentId;
    }

    /**
     * @return Host names.
     */
    public Collection<String> hostNames() {
        return hostNames;
    }

    /**
     * @return CPU load in percents.
     */
    public double cpuLoadPercent() {
        return cpuLoadPercent;
    }

    /**
     * @return Load average.
     */
    public double loadAverage() {
        return loadAverage;
    }

    /**
     * @return Garbage collector CPU load in percents.
     */
    public double gcCpuLoadPercent() {
        return gcCpuLoadPercent;
    }

    /**
     * @return Heap usage in percents.
     */
    public double heapUsagePercent() {
        return heapUsagePercent;
    }

    /**
     * @return Data regions usages in percents.
     */
    public Map<String, Double> dataRegionUsagesPercents() {
        return Collections.unmodifiableSortedMap(dataRegionUsagesPercents);
    }

    /**
     * @return Data storage size in gigabytes.
     */
    public double dataStorageSizeGigabytes() {
        return dataStorageSizeGigabytes;
    }
}