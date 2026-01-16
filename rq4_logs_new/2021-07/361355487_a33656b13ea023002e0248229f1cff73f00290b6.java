package org.testmy.data;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.sforce.soap.partner.sobject.SObject;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
import org.testmy.error.TestRuntimeException;

public class SalesforceDataCache {
    private List<SObject> data = new LinkedList<>();

    public Optional<SObject> findObject(final Matcher<SObject> objectShape) {
        return data.stream().filter(el -> objectShape.matches(el)).findFirst();
    }

    public void addOjbects(final SObject... sObjects) {
        final List<SObject> objectsWithoutId = new ArrayList<>();
        final List<SObject> objectsWithoutType = new ArrayList<>();
        for (final SObject sObject : sObjects) {
            if (StringUtils.isBlank(sObject.getId())) {
                objectsWithoutId.add(sObject);
            }
            else if (StringUtils.isBlank(sObject.getType())) {
                objectsWithoutType.add(sObject);
            }
            else {
                data.add(sObject);
            }
        }
        if (!objectsWithoutId.isEmpty()) {
            throw new TestRuntimeException("Cannot add objects without Id: " + objectsWithoutId);
        }
        if (!objectsWithoutType.isEmpty()) {
            throw new TestRuntimeException("Cannot add objects without Type: " + objectsWithoutType);
        }
    }

    public List<SObject> findObjects(final Matcher<SObject> objectShape) {
        return data.stream().filter(el -> objectShape.matches(el)).collect(Collectors.toList());
    }

    public Set<String> getIds() {
        return data.stream().map(el -> el.getId()).collect(Collectors.toSet());
    }
}