/*
 * Copyright 2017 Hewlett-Packard Development Company, L.P.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hp.mqm.client.model;

/**
 * Class used to pass plugin info during retrieving Abridged tasks from server
 */
public class AbridgedTaskPluginInfo {
    private String selfIdentity;
    private String selfType;
    private String selfLocation;
    private Integer apiVersion;
    private String sdkVersion;
    private String pluginVersion;
    private String ciServerUser;
    private String octaneUser;

    public String getSelfIdentity() {
        return selfIdentity;
    }

    public AbridgedTaskPluginInfo setSelfIdentity(String selfIdentity) {
        this.selfIdentity = selfIdentity;
        return this;
    }

    public String getSelfType() {
        return selfType;
    }

    public AbridgedTaskPluginInfo setSelfType(String selfType) {
        this.selfType = selfType;
        return this;
    }

    public String getSelfLocation() {
        return selfLocation;
    }

    public AbridgedTaskPluginInfo setSelfLocation(String selfLocation) {
        this.selfLocation = selfLocation;
        return this;
    }

    public Integer getApiVersion() {
        return apiVersion;
    }

    public AbridgedTaskPluginInfo setApiVersion(Integer apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public String getSdkVersion() {
        return sdkVersion;
    }

    public AbridgedTaskPluginInfo setSdkVersion(String sdkVersion) {
        this.sdkVersion = sdkVersion;
        return this;
    }

    public String getPluginVersion() {
        return pluginVersion;
    }

    public AbridgedTaskPluginInfo setPluginVersion(String pluginVersion) {
        this.pluginVersion = pluginVersion;
        return this;
    }

    public String getCiServerUser() {
        return ciServerUser;
    }

    public AbridgedTaskPluginInfo setCiServerUser(String ciServerUser) {
        this.ciServerUser = ciServerUser;
        return this;
    }

    public String getOctaneUser() {
        return octaneUser;
    }

    public AbridgedTaskPluginInfo setOctaneUser(String octaneUser) {
        this.octaneUser = octaneUser;
        return this;
    }
}