/*
 * Copyright (c) 2015 Clare Controls. All rights reserved.
 *
 * This software is the confidential and proprietary information of Clare Controls ("Confidential Information"). You
 * shall not disclose or reproduce such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Clare Controls.
 */
package com.clarecontrols.equator.solstice.api.beta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.persistence.EntityManager;

import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.clarecontrols.equator.solstice.api.APIException;
import com.clarecontrols.equator.solstice.api.APIException.APIErrorCode;
import com.clarecontrols.equator.solstice.api.model.ImportResult;
import com.clarecontrols.equator.solstice.db.entities.DeviceCategory;
import com.clarecontrols.equator.solstice.db.entities.DeviceClass;
import com.clarecontrols.equator.solstice.db.entities.DeviceItem;
import com.clarecontrols.equator.solstice.db.entities.DeviceType;
import com.clarecontrols.equator.solstice.db.entities.Project;
import com.clarecontrols.equator.solstice.db.entities.ProjectVersion;
import com.clarecontrols.equator.solstice.db.entities.ProtocolAdapter;
import com.clarecontrols.equator.solstice.db.entities.Zone;
import com.clarecontrols.equator.solstice.db.entities.roles.CompanyGroup;
import com.clarecontrols.equator.solstice.db.entities.roles.User;
import com.clarecontrols.equator.solstice.db.entities.rules.CFNode;
import com.clarecontrols.equator.solstice.db.entities.rules.CFProvider;
import com.clarecontrols.equator.solstice.db.entities.services.ServiceDefinition;
import com.clarecontrols.equator.solstice.db.util.QueryHelper;
import com.clarecontrols.equator.solstice.db.util.ServicesHelper;

/**
 * ImportManager represents the import manager.
 */
public final class ImportManager {

    private ProjectVersion version;
    private final EntityManager entityManager;
    private final Map<Integer, CFNode> idCFNodeMap = new HashMap<>();
    private final Map<Integer, DeviceItem> idDeviceMap = new HashMap<>();
    private final Map<Integer, Zone> idZoneMap = new HashMap<>();
    private final Map<String, CFProvider> nameTypeCFProviderMap = new HashMap<>();
    private final Map<String, DeviceCategory> nameCategoryMap = new HashMap<>();
    private final Map<String, DeviceClass> nameDeviceClassMap = new HashMap<>();
    private final Map<String, DeviceType> keyDeviceTypeMap = new HashMap<>();
    private final Map<String, ProtocolAdapter> keyProtocolAdapterMap = new HashMap<>();
    private final Map<String, ServiceDefinition> keyServiceDefinitionMap = new HashMap<>();
    private final Map<String, User> emailUserMap = new HashMap<>();
    private final Set<String> errors = new HashSet<String>();

    /**
     * @param eManager entity manager
     * @param jsonData json string to import from
     * @return import result
     */
    public static ImportResult importProjectVersion(final EntityManager eManager, final String jsonData) {
        try {
            final JSONObject jsonProjectVersion = new JSONObject(jsonData);
            final ImportManager manager = new ImportManager(eManager);
            JZone.importZones(manager, jsonProjectVersion);
            JProjectVersion.importProjectVersion(manager, jsonProjectVersion.getJSONObject(JKey.PROJECT_VERSION));
            // Utils.assertProjectPermission(user, Permission.EditProject, manager.getVersion().getProject());
            JDeviceItem.importDeviceItems(manager, jsonProjectVersion);
            JCFNode.importCFNodes(manager, jsonProjectVersion);
            JServiceInstance.importServiceInstances(manager, jsonProjectVersion);
            final ImportResult result = new ImportResult();
            result.setProjectVersion(manager.getVersion());
            result.setErrors(manager.getErrors());
            return result;
        } catch (final JSONException ex) {
            throw new APIException(APIErrorCode.GENERIC_ERROR, ex);
        }
    }

    /**
     * Constructor. (it should be "private"; package access due to unit-testing purpose)
     * @param eManager
     */
    ImportManager(final EntityManager entityManager) {
        this.entityManager = entityManager;
        initCFProviderLookup();
        initDeviceCategoryLookup();
        initDeviceClassLookup();
        initDeviceTypeLookup();
        initProtocolAdapterLookup();
        initServiceDefinitionLookup();
    }

    /**
     * @return The eManager.
     */
    EntityManager getEntityManager() {
        return entityManager;
    }

    /**
     * @return The idZoneMap.
     */
    Map<Integer, Zone> getIdZoneMap() {
        return idZoneMap;
    }

    /**
     * @return The idDeviceMap.
     */
    Map<Integer, DeviceItem> getIdDeviceMap() {
        return idDeviceMap;
    }

    /**
     * @return The idCFNodeMap.
     */
    Map<Integer, CFNode> getIdCFNodeMap() {
        return idCFNodeMap;
    }

    /**
     * @return The version.
     */
    ProjectVersion getVersion() {
        return version;
    }

    /**
     * @param version - The version to set.
     */
    void setVersion(final ProjectVersion version) {
        this.version = version;
    }

    /**
     * @param error - Error message to add into error list
     */
    void addError(final String error) {
        errors.add(error);
    }

    /**
     * @return The errors.
     */
    List<String> getErrors() {
        final List<String> errorMsgs = new ArrayList<>(errors);
        Collections.sort(errorMsgs);
        return errorMsgs;
    }

    /**
     * Initialize CFProvider lookup map.
     */
    private void initCFProviderLookup() {
        final List<CFProvider> providers = QueryHelper.getCFProviders(entityManager);
        for (final CFProvider provider : providers) {
            final String providerName = provider.getName();
            final String providerTypeName = provider.getProviderType().name();
            final String key = providerName + LOOKUP_KEY_SEPARATOR + providerTypeName;
            if (nameTypeCFProviderMap.put(key, provider) != null) {
                errors.add(String.format(ERROR_CFPROVIDER_NOT_UNIQUE, providerName, providerTypeName));
            }
        }
    }

    /**
     * @param name CFProvider name
     * @param typeName CFProvider's type name
     * @return CFProvider correspond
     */
    CFProvider lookupCFProvider(final String name, final String typeName) {
        CFProvider provider = null;
        if (name != null) {
            provider = nameTypeCFProviderMap.get(name + LOOKUP_KEY_SEPARATOR + typeName);
            if (provider == null) {
                addError(String.format(ERROR_CFPROVIDER_NOT_FOUND, name, typeName));
            }
        }
        return provider;
    }

    /**
     * Initialize device category lookup map.
     */
    private void initDeviceCategoryLookup() {
        final List<DeviceCategory> categories = QueryHelper.getDeviceCategories(entityManager, false);
        for (final DeviceCategory category : categories) {
            if (nameCategoryMap.put(category.getName(), category) != null) {
                errors.add(String.format(ERROR_DEVICE_CATEGORY_NOT_UNIQUE, category.getName()));
            }
        }
    }

    /**
     * @param name device category name
     * @return device category correspond
     */
    DeviceCategory lookupDeviceCategory(final String name) {
        DeviceCategory category = null;
        if (name != null) {
            category = nameCategoryMap.get(name);
            if (category == null) {
                addError(String.format(ERROR_DEVICE_CATEGORY_NOT_FOUND, name));
            }
        }
        return category;
    }

    /**
     * Initialize device class lookup map.
     */
    private void initDeviceClassLookup() {
        final List<DeviceClass> deviceClasses = QueryHelper.getDeviceClasses(entityManager);
        for (final DeviceClass deviceClass : deviceClasses) {
            if (nameDeviceClassMap.put(deviceClass.getName(), deviceClass) != null) {
                errors.add(String.format(ERROR_DEVICE_CLASS_NOT_UNIQUE, deviceClass.getName()));
            }
        }
    }

    /**
     * @param name device class's name
     * @return device class correspond
     */
    DeviceClass lookupDeviceClass(final String name) {
        DeviceClass deviceClass = null;
        if (name != null) {
            deviceClass = nameDeviceClassMap.get(name);
            if (deviceClass == null) {
                addError(String.format(ERROR_DEVICE_CLASS_NOT_FOUND, name));
            }
        }
        return deviceClass;
    }

    /**
     * Initialize device type lookup map.
     */
    private void initDeviceTypeLookup() {
        final List<DeviceType> deviceTypes = QueryHelper.getDeviceTypes(entityManager);
        for (final DeviceType deviceType : deviceTypes) {
            final String typeName = deviceType.getName();
            final String categoryName = deviceType.getDeviceCategory().getName();
            final String key = typeName + LOOKUP_KEY_SEPARATOR + categoryName;
            if (keyDeviceTypeMap.put(key, deviceType) != null) {
                errors.add(String.format(ERROR_DEVICE_TYPE_NOT_UNIQUE, typeName, categoryName));
            }
        }
    }

    /**
     * @param deviceTypeName device type's name
     * @param categoryName category's name
     * @return device type correspond
     */
    DeviceType lookupDeviceType(final String deviceTypeName, final String categoryName) {
        DeviceType deviceType = null;
        if (deviceTypeName != null && categoryName != null) {
            deviceType = keyDeviceTypeMap.get(deviceTypeName + LOOKUP_KEY_SEPARATOR + categoryName);
            if (deviceType == null) {
                errors.add(String.format(ERROR_DEVICE_TYPE_NOT_FOUND, deviceTypeName, categoryName));
            }
        }
        return deviceType;
    }

    /**
     * @param projectName project name
     * @param companyName company name
     * @return project correspond
     */
    Project lookupProject(final String projectName, final String companyName) {
        Project project = null;
        if (projectName != null) {
            final CompanyGroup group = QueryHelper.getCompanyGroup(entityManager, companyName);
            if (group != null) {
                project = QueryHelper.getProject(entityManager, projectName, group.getId());
            }
            if (project == null) {
                errors.add(String.format(ERROR_PROJECT_NOT_FOUND, projectName, companyName));
            }
        }
        return project;
    }

    /**
     * Initialize protocol adapter lookup map.
     */
    private void initProtocolAdapterLookup() {
        final List<ProtocolAdapter> protocolAdapters = QueryHelper.getProtocolAdapters(entityManager);
        for (final ProtocolAdapter protocol : protocolAdapters) {
            final String key = protocol.getName() + LOOKUP_KEY_SEPARATOR + protocol.getVersion();
            if (keyProtocolAdapterMap.put(key, protocol) != null) {
                errors.add(String.format(ERROR_PROTOCOL_ADAPTER_NOT_UNIQUE, protocol.getName(), protocol.getVersion()));
            }
        }
    }

    /**
     * @param name protocol adapter's name
     * @param version protocol adapter's version
     * @return protocol adapter correspond
     */
    ProtocolAdapter lookupProtocolAdapter(final String name, final String version) {
        ProtocolAdapter protocol = null;
        if (name != null && version != null) {
            protocol = keyProtocolAdapterMap.get(name + LOOKUP_KEY_SEPARATOR + version);
            if (protocol == null) {
                errors.add(String.format(ERROR_PROTOCOL_ADAPTER_NOT_FOUND, name, version));
            }
        }
        return protocol;
    }

    /**
     * Initialize service definition lookup map.
     */
    private void initServiceDefinitionLookup() {
        final List<ServiceDefinition> serviceDefinitions = ServicesHelper.getServiceDefinitions(entityManager);
        for (final ServiceDefinition serviceDef : serviceDefinitions) {
            final String sdUid = serviceDef.getUid();
            final String sdVendor = serviceDef.getVendor();
            final String sdVersion = serviceDef.getVersion();
            final String key = sdUid + LOOKUP_KEY_SEPARATOR + sdVendor + LOOKUP_KEY_SEPARATOR + sdVersion;
            if (keyServiceDefinitionMap.put(key, serviceDef) != null) {
                errors.add(String.format(ERROR_SERVICE_DEFINITION_NOT_UNIQUE, sdUid, sdVendor, sdVersion));
            }
        }
    }

    /**
     * @param name service definition name
     * @param uid service definition uid
     * @param vendor service definition vendor
     * @param version service definition version
     * @return service definition correspond
     */
    ServiceDefinition lookupServiceDefinition(final String name, final String uid, final String vendor,
        final String version) {
        ServiceDefinition serviceDefinition = null;
        if (uid != null) {
            final String key = uid + LOOKUP_KEY_SEPARATOR + vendor + LOOKUP_KEY_SEPARATOR + version;
            serviceDefinition = keyServiceDefinitionMap.get(key);
            if (serviceDefinition == null) {
                addError(String.format(ERROR_SERVICE_DEFINITION_NOT_FOUND, name, uid, vendor, version));
            }
        }
        return serviceDefinition;
    }

    /**
     * @param name template name
     * @param vendor template vendor
     * @param modelNumber template model number
     * @param version template version
     * @return template correspond
     */
    DeviceItem lookupTemplate(final String name, final String vendor, final String modelNumber, final String version) {
        final List<DeviceItem> result = new ArrayList<>();
        if (name != null) {
            final List<DeviceItem> templates = QueryHelper.getTemplatesByName(entityManager, name);
            for (final DeviceItem template : templates) {
                if (StringUtils.equals(template.getVendor(), vendor)
                    && StringUtils.equals(template.getModelNumber(), modelNumber)
                    && StringUtils.equals(template.getVersion(), version)) {
                    result.add(template);
                }
            }
            if (result.isEmpty()) {
                addError(String.format(ERROR_TEMPLATE_NOT_FOUND, name, vendor, modelNumber, version));
            } else if (result.size() > 1) {
                addError(String.format(ERROR_TEMPLATE_NOT_UNIQUE, name, vendor, modelNumber, version));
            }
        }
        return result.isEmpty() ? null : result.get(0);
    }

    /**
     * @param email user's email
     * @return user correspond
     */
    User lookupUser(final String email) {
        User user = null;
        if (email != null) {
            user = emailUserMap.get(email);
            if (user == null) {
                user = QueryHelper.getUser(entityManager, email);
                if (user != null) {
                    emailUserMap.put(email, user);
                } else {
                    addError(String.format(ERROR_USER_NOT_FOUND, email));
                }
            }
        }
        return user;
    }

    /**
     * @param entityClass entity class
     * @param entityId entity id
     * @return entity correspond
     */
    <T> T findEntity(final Class<T> entityClass, final int entityId) {
        Objects.requireNonNull(entityClass);
        final T entity = entityManager.find(entityClass, entityId);
        if (entity == null) {
            addError(String.format(ERROR_ENTITY_NOT_FOUND, entityClass.getSimpleName(), entityId));
        }
        return entity;
    }

    private static final String LOOKUP_KEY_SEPARATOR = "$$$";

    private static final String ERROR_CFPROVIDER_NOT_FOUND = //
        "CFProvider not found [name=\"%s\", typeName=\"%s\"]";

    private static final String ERROR_CFPROVIDER_NOT_UNIQUE = //
        "CFProvider not unique [name=\"%s\", typeName=\"%s\"]";

    private static final String ERROR_DEVICE_CATEGORY_NOT_FOUND = //
        "DeviceCategory not found [name=\"%s\"]";

    private static final String ERROR_DEVICE_CATEGORY_NOT_UNIQUE = //
        "DeviceCategory not unique [name=\"%s\"]";

    private static final String ERROR_DEVICE_CLASS_NOT_FOUND = //
        "DeviceClass not found [name=\"%s\"]";

    private static final String ERROR_DEVICE_CLASS_NOT_UNIQUE = //
        "DeviceClass not unique [name=\"%s\"]";

    private static final String ERROR_DEVICE_TYPE_NOT_FOUND = //
        "DeviceType not found [deviceTypeName=\"%s\", categoryName=\"%s\"]";

    private static final String ERROR_DEVICE_TYPE_NOT_UNIQUE = //
        "DeviceType not unique [deviceTypeName=\"%s\", categoryName=\"%s\"]";

    private static final String ERROR_ENTITY_NOT_FOUND = //
        "Entity not found [entityClass=\"%s\", entityId=%d]";

    private static final String ERROR_PROJECT_NOT_FOUND = //
        "Project not found [projectName=\"%s\", companyName=\"%s\"]";

    private static final String ERROR_PROTOCOL_ADAPTER_NOT_FOUND = //
        "ProtocolAdapter not found [name=\"%s\", version=\"%s\"]";

    private static final String ERROR_PROTOCOL_ADAPTER_NOT_UNIQUE = //
        "ProtocolAdapter not unique [name=\"%s\", version=\"%s\"]";

    private static final String ERROR_SERVICE_DEFINITION_NOT_FOUND = //
        "ServiceDefinition not found [name=\"%s\", uid=\"%s\", vendor=\"%s\", version=\"%s\"]";

    private static final String ERROR_SERVICE_DEFINITION_NOT_UNIQUE = //
        "ServiceDefinition not unique [uid=\"%s\", vendor=\"%s\", version=\"%s\"]";

    private static final String ERROR_TEMPLATE_NOT_FOUND = //
        "Template not found [name=\"%s\", vendor=\"%s\", modelNumber=\"%s\", version=\"%s\"]";

    private static final String ERROR_TEMPLATE_NOT_UNIQUE = //
        "Template not unique [name=\"%s\", vendor=\"%s\", modelNumber=\"%s\", version=\"%s\"]";

    private static final String ERROR_USER_NOT_FOUND = //
        "User not found [email=\"%s\"]";
}