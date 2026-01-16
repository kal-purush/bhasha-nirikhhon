/* tslint:disable */
import { NgModule, ModuleWithProviders, SkipSelf, Optional } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { ApiConfiguration, ApiConfigurationParams } from './api-configuration';

import { UtilsService } from './services/utils.service';
import { AuthzResolverService } from './services/authz-resolver.service';
import { AttributesManagerService } from './services/attributes-manager.service';
import { DatabaseManagerService } from './services/database-manager.service';
import { ExtSourcesManagerService } from './services/ext-sources-manager.service';
import { UsersManagerService } from './services/users-manager.service';
import { MembersManagerService } from './services/members-manager.service';
import { FacilitiesManagerService } from './services/facilities-manager.service';
import { OwnersManagerService } from './services/owners-manager.service';
import { GroupsManagerService } from './services/groups-manager.service';
import { ResourcesManagerService } from './services/resources-manager.service';
import { VosManagerService } from './services/vos-manager.service';
import { RegistrarManagerService } from './services/registrar-manager.service';
import { ServicesManagerService } from './services/services-manager.service';

/**
 * Module that provides all services and configuration.
 */
@NgModule({
  imports: [],
  exports: [],
  declarations: [],
  providers: [
    UtilsService,
    AuthzResolverService,
    AttributesManagerService,
    DatabaseManagerService,
    ExtSourcesManagerService,
    UsersManagerService,
    MembersManagerService,
    FacilitiesManagerService,
    OwnersManagerService,
    GroupsManagerService,
    ResourcesManagerService,
    VosManagerService,
    RegistrarManagerService,
    ServicesManagerService,
    ApiConfiguration
  ],
})
export class ApiModule {
  static forRoot(params: ApiConfigurationParams): ModuleWithProviders {
    return {
      ngModule: ApiModule,
      providers: [
        {
          provide: ApiConfiguration,
          useValue: params
        }
      ]
    }
  }

  constructor( 
    @Optional() @SkipSelf() parentModule: ApiModule,
    @Optional() http: HttpClient
  ) {
    if (parentModule) {
      throw new Error('ApiModule is already loaded. Import in your base AppModule only.');
    }
    if (!http) {
      throw new Error('You need to import the HttpClientModule in your AppModule! \n' +
      'See also https://github.com/angular/angular/issues/20575');
    }
  }
}