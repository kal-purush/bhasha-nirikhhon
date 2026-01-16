package com.zm.beans;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ConstructorArgument {

    private final List<ValueHolder> argumentValues = new LinkedList<ValueHolder>();


    /**
     * Create a new empty ConstructorArgumentValues object.
     */
    public ConstructorArgument() {
    }


    public void addArgumentValue(ValueHolder valueHolder) {
        this.argumentValues.add(valueHolder);
    }

    public List<ValueHolder> getArgumentValues() {
        return Collections.unmodifiableList(this.argumentValues);
    }


    public int getArgumentCount() {
        return this.argumentValues.size();
    }

    public boolean isEmpty() {
        return this.argumentValues.isEmpty();
    }

    /**
     * Clear this holder, removing all argument values.
     */
    public void clear() {
        this.argumentValues.clear();
    }


    /**
     * Holder for a constructor argument value, with an optional type
     * attribute indicating the target type of the actual constructor argument.
     */
    public static class ValueHolder {

        private Object value;

        public ValueHolder(Object value) {
            this.value = value;
        }


        public void setValue(Object value) {
            this.value = value;
        }


        public Object getValue() {
            return this.value;
        }
        
    }

}