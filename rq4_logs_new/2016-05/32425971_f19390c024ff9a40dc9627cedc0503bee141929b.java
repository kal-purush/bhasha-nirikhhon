/*
 * Copyright 2003 - 2016 The eFaps Team
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
 *
 */

package org.efaps.json.index.result;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO comment!
 *
 * @author The eFaps Team
 */
public class DimValue
    implements Serializable
{

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The label values. */
    private final Set<DimValue> children = new HashSet<>();

    /** The label. */
    private String label;

    /** The value. */
    private Integer value;

    /**
     * Getter method for the instance variable {@link #label}.
     *
     * @return value of instance variable {@link #label}
     */
    public String getLabel()
    {
        return this.label;
    }

    /**
     * Setter method for instance variable {@link #label}.
     *
     * @param _label value for instance variable {@link #label}
     * @return the dim value
     */
    public DimValue setLabel(final String _label)
    {
        this.label = _label;
        return this;
    }

    /**
     * Getter method for the instance variable {@link #value}.
     *
     * @return value of instance variable {@link #value}
     */
    public Integer getValue()
    {
        return this.value;
    }

    /**
     * Setter method for instance variable {@link #value}.
     *
     * @param _value value for instance variable {@link #value}
     * @return the dim value
     */
    public DimValue setValue(final Integer _value)
    {
        this.value = _value;
        return this;
    }

    /**
     * Getter method for the instance variable {@link #children}.
     *
     * @return value of instance variable {@link #children}
     */
    public Set<DimValue> getChildren()
    {
        return this.children;
    }

    @Override
    public String toString()
    {
        return String.format("Label: %s, Value: %s, Children: %s", getLabel(), getValue(), getChildren());
    }
}