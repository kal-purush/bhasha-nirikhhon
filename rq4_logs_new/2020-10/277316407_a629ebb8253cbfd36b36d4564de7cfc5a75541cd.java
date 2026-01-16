package ru.otus.hibernate;

import org.hibernate.cfg.Configuration;
import ru.otus.hibernate.exceptions.CreatingConfigurationException;

public class FactoryConfigurationHibernateByResource implements FactoryConfigurationHibernate {

    private final String pathToResource;

    public FactoryConfigurationHibernateByResource(String pathToResource) {
        this.pathToResource = pathToResource;
    }

    @Override
    public Configuration createConfiguration() {
        try {
            return new Configuration().configure(pathToResource);
        } catch (Exception e) {
            throw new CreatingConfigurationException(e);
        }
    }
}