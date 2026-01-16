package de.aservo.confapi.crowd.exception;

import de.aservo.confapi.commons.exception.NotFoundException;
import de.aservo.confapi.commons.model.AbstractDirectoryBean;

@SuppressWarnings("java:S110")
public class NotFoundExceptionForDirectory extends NotFoundException {

    public NotFoundExceptionForDirectory(
            final AbstractDirectoryBean directoryBean) {

        this(directoryBean.getName());
    }

    public NotFoundExceptionForDirectory(
            final String name) {

        super(String.format("Directory with name '%s' could not be found", name));
    }

    public NotFoundExceptionForDirectory(
            final long id) {

        super(String.format("Directory with id '%d' could not be found", id));
    }

}