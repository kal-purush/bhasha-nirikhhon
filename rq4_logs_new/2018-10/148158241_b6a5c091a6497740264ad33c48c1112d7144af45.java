/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2018, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package io.narayana.openshift.txrecovery.hibernate;

import java.util.Optional;
import java.util.Properties;

import io.narayana.openshift.txrecovery.cliargs.ParsedArguments;

/**
 * Taking properties from system and parsed arguments
 * and link them the hibernate properties.
 */
public final class HibernateProperties {
    private HibernateProperties() {
        // utility class
    }

    private static final String DB_TABLE_NAME_PARAM = "db.table.name";
    private static final String HIBERNATE_DIALECT_PARAM = "hibernate.dialect";
    private static final String HIBERNATE_CONNECTION_DRIVER_CLASS_PARAM = "hibernate.connection.driver_class";
    private static final String HIBERNATE_CONNECTION_URL_PARAM = "hibernate.connection.url";
    private static final String HIBERNATE_CONNECTION_USERNAME_PARAM = "hibernate.connection.username";
    private static final String HIBERNATE_CONNECTION_PASSWORD_PARAM = "hibernate.connection.password";

    /**
     * <p>
     * Gathering properties from the parsed command line arguments
     * and link them for Hibernate to start.
     * <p>
     * It uses {@link ParsedArguments} to get command line values.
     *
     * @param args  values to  be used as the most important for the setup
     * @return  properties for hibernate to be setup
     */
    public static Properties setupPropertiesByParsedArguments(ParsedArguments args) {
        Properties outputProperties = prepareHibernateBasicProperties();
        setIfNotNull(HIBERNATE_DIALECT_PARAM, args.getHibernateDialect(), outputProperties);
        setIfNotNull(HIBERNATE_CONNECTION_DRIVER_CLASS_PARAM, args.getJdbcDriverClass(), outputProperties);
        setIfNotNull(HIBERNATE_CONNECTION_URL_PARAM, args.getJdbcUrl(), outputProperties);
        setIfNotNull(HIBERNATE_CONNECTION_USERNAME_PARAM, args.getUser(), outputProperties);
        setIfNotNull(HIBERNATE_CONNECTION_PASSWORD_PARAM, args.getPassword(), outputProperties);
        setIfNotNull(DB_TABLE_NAME_PARAM, args.getTableName(), outputProperties);
        return outputProperties;
    }

    /**
     * Loading environmental and system properties
     * which are used for hibernate setup.
     *
     * @return hibernate properties loaded from env/system properties
     */
    private static Properties prepareHibernateBasicProperties() {
        Properties outputProperties = new Properties();

        getAndWriteProperty(HIBERNATE_DIALECT_PARAM, outputProperties);
        getAndWriteProperty(HIBERNATE_CONNECTION_DRIVER_CLASS_PARAM, outputProperties);
        getAndWriteProperty(HIBERNATE_CONNECTION_URL_PARAM, outputProperties);
        getAndWriteProperty(HIBERNATE_CONNECTION_USERNAME_PARAM, outputProperties);
        getAndWriteProperty(HIBERNATE_CONNECTION_PASSWORD_PARAM, outputProperties);
        getAndWriteProperty(DB_TABLE_NAME_PARAM, outputProperties);
        return outputProperties;
    }

    /**
     * Returning table name which is currently used for saving the recovery markers.
     *
     * @param setupProperties  properties to search for the db table name
     * @return name of table used in app
     */
    public static String getTableName(Properties setupProperties) {
        String appRecoveryPodTableName = setupProperties.getProperty(DB_TABLE_NAME_PARAM);
        if(appRecoveryPodTableName == null) appRecoveryPodTableName = ApplicationRecoveryPod.TABLE_NAME;
        return appRecoveryPodTableName;
    }


    /**
     * Search of environment properties and system properties for the {@code key}.
     */
    private static Optional<String> getProperty(String key) {
        if(key == null) throw new NullPointerException("key");
        String property = System.getProperty(key);
        if(property == null) property = System.getenv(key);
        if(property == null) property = System.getProperty(key.toLowerCase().replaceAll("_", "."));
        if(property == null) property = System.getenv(key.toUpperCase().replaceAll("[.]", "_"));
        return Optional.ofNullable(property);
    }

    /**
     * Get property value and if it's found it's written to the outputProperties
     */
    private static Optional<String> getAndWriteProperty(String key, final Properties propertiesToWriteIn) {
        Optional<String> value = getProperty(key);
        if(value.isPresent()) propertiesToWriteIn.setProperty(key, value.get());
        return value;
    }

    private static Properties setIfNotNull(String key, String value, final Properties propertiesToChange) {
        if(key != null && value != null && !key.isEmpty() && !value.isEmpty())
            propertiesToChange.setProperty(key, value);
        return propertiesToChange;
    }
}