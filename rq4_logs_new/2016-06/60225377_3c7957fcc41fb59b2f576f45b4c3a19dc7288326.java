/*
 * Copyright 2016-present Open Networking Laboratory
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

package org.opencord.cordconfig;

import org.onosproject.event.AbstractEvent;
import org.joda.time.LocalDateTime;
import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Describes an CORD config event.
 */
public class CordConfigEvent extends AbstractEvent<CordConfigEvent.Type, Object> {
    private final Object prevSubject;

    public enum Type {
        /**
         * Indicates a new access agent has been added.
         * Event subject should be AccessAgentData.
         */
        ACCESS_AGENT_ADDED,

        /**
         * Indicates an access agent has been updated.
         * Event subject and prevSubject should be AccessAgentData.
         */
        ACCESS_AGENT_UPDATED,

        /**
         * Indicates an access agent has been removed.
         * Event prevSubject should be AccessAgentData.
         */
        ACCESS_AGENT_REMOVED,

        /**
         * Indicates a new access device has been added.
         * Event subject should be AccessDeviceData.
         */
        ACCESS_DEVICE_ADDED,

        /**
         * Indicates an access device has been updated.
         * Event subject and prevSubject should be AccessDeviceData.
         */
        ACCESS_DEVICE_UPDATED,

        /**
         * Indicates an access device has been removed.
         * Event prevSubject should be AccessDeviceData.
         */
        ACCESS_DEVICE_REMOVED,
    }

    /**
     * Creates an CORD config event with type and subject.
     *
     * @param type event type
     * @param subject subject CORD config
     */
    public CordConfigEvent(Type type, Object subject) {
        this(type, subject, null);
    }

    /**
     * Creates an CORD config event with type, subject and time of event.
     *
     * @param type event type
     * @param subject subject CORD config
     * @param time time of event
     */
    public CordConfigEvent(Type type, Object subject, long time) {
        this(type, subject, null, time);
    }

    /**
     * Creates an CORD config event with type, subject and previous subject.
     *
     * @param type event type
     * @param subject subject CORD config
     * @param prevSubject previous CORD config subject
     */
    public CordConfigEvent(Type type, Object subject, Object prevSubject) {
        super(type, subject);
        this.prevSubject = prevSubject;
    }

    /**
     * Creates an CORD config event with type, subject, previous subject and time.
     *
     * @param type event type
     * @param subject subject CORD config
     * @param prevSubject previous CORD config subject
     * @param time time of event
     */
    public CordConfigEvent(Type type, Object subject, Object prevSubject, long time) {
        super(type, subject, time);
        this.prevSubject = prevSubject;
    }

    /**
     * Returns the previous CORD config subject.
     *
     * @return previous subject of CORD config or null if previous subject does not exist.
     */
    public Object prevSubject() {
        return prevSubject;
    }

    @Override
    public String toString() {
        if (prevSubject == null) {
            return super.toString();
        }
        return toStringHelper(this)
                .add("time", new LocalDateTime(time()))
                .add("type", type())
                .add("subject", subject())
                .add("prevSubject", prevSubject)
                .toString();
    }
}