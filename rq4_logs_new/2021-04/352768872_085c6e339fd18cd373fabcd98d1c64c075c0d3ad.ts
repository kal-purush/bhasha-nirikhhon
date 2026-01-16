import {
  createDirectRelationship,
  createIntegrationEntity,
  Entity,
  IntegrationStep,
  IntegrationStepExecutionContext,
  RelationshipClass,
  //parseTimePropertyValue,
  //IntegrationMissingKeyError,
} from '@jupiterone/integration-sdk-core';

import { createAPIClient } from '../client';
import { IntegrationConfig } from '../config';
import { DATA_ACCOUNT_ENTITY } from './account';

export const APPLICATION_ENTITY_TYPE = 'okta_application';

export async function fetchApplications({
  instance,
  jobState,
  logger,
}: IntegrationStepExecutionContext<IntegrationConfig>) {
  const apiClient = createAPIClient(instance.config, logger);

  const accountEntity = (await jobState.getData(DATA_ACCOUNT_ENTITY)) as Entity;

  await apiClient.iterateApplications(async (app) => {
    /*const webLink = url.resolve(
      getOktaAccountAdminUrl(instance.config),
      `/admin/group/${group.id}`,
    );*/
    /*const entityType =
      group.type === 'APP_GROUP'
        ? APP_USER_GROUP_ENTITY_TYPE
        : USER_GROUP_ENTITY_TYPE;*/

    const appEntity = await jobState.addEntity(
      createIntegrationEntity({
        entityData: {
          source: app,
          assign: {
            _key: app.id,
            _type: APPLICATION_ENTITY_TYPE,
            _class: 'Application',
            id: app.id,
            /*webLink: webLink,
            displayName: group.profile.name,
            created: parseTimePropertyValue(group.created)!,
            createdOn: parseTimePropertyValue(group.created)!,
            lastUpdated: parseTimePropertyValue(group.lastUpdated)!,
            lastUpdatedOn: parseTimePropertyValue(group.lastUpdated)!,
            lastMembershipUpdated: parseTimePropertyValue(
              group.lastMembershipUpdated,
            )!,
            lastMembershipUpdatedOn: parseTimePropertyValue(
              group.lastMembershipUpdated,
            )!,
            objectClass: group.objectClass,
            type: group.type,
            name: group.profile.name,
            description: group.profile.description,*/
          },
        },
      }),
    );

    //TODO: change the below to be right
    await jobState.addRelationship(
      createDirectRelationship({
        _class: RelationshipClass.HAS,
        from: accountEntity,
        to: appEntity,
      }),
    );
  });
}
export const applicationSteps: IntegrationStep<IntegrationConfig>[] = [
  {
    id: 'fetch-applications',
    name: 'Fetch Applications',
    entities: [
      {
        resourceName: 'Okta Application',
        _type: APPLICATION_ENTITY_TYPE,
        _class: 'Application',
      },
    ],
    relationships: [
      {
        _type: 'okta_account_has_application',
        _class: RelationshipClass.HAS,
        sourceType: 'okta_account',
        targetType: APPLICATION_ENTITY_TYPE,
      },
    ],
    dependsOn: ['fetch-account'],
    executionHandler: fetchApplications,
  },
];