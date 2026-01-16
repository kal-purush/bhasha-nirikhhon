import { Controller, Param, Get, Post } from '@nestjs/common';
import { ConfigurationContext } from 'twilio/lib/rest/conversations/v1/service/configuration';
import ConfigurationService from '../services/configuration.service';

@Controller('/configurations')
export default class ConfigurationController {
  constructor(private readonly configurationService: ConfigurationService) {}

  @Get('/:serviceId')
  async listConfigurations(
    @Param('serviceId') serviceId: string,
  ): Promise<ConfigurationContext> {
    return await this.configurationService.listConfigurations(serviceId);
  }

  @Get('/:serviceId/:reachabilityValue')
  async changeReachability(
    @Param('serviceId') serviceId: string,
    @Param('reachabilityValue') reachabilityValue: boolean,
  ): Promise<ConfigurationContext> {
    return await this.configurationService.changeReachability(
      serviceId,
      reachabilityValue,
    );
  }
}