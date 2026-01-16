import { Microorganism } from '../model/microorganism.model';

// npm
import { inject, injectable } from 'inversify';
import * as _ from 'lodash';
import {
    FilterConfiguration,
    FilterDefinition,
    FilterConfigurationService
} from '../model/filter.model';
import { APPLICATION_TYPES } from '../../application.types';
import { Matrix } from '../model/matrix.model';
import { SamplingContext } from '../model/sampling-context.model';
import { EntityGateway } from '../model/shared.model';

@injectable()
export class DefaultFilterConfigurationService
    implements FilterConfigurationService {
    constructor(
        @inject(APPLICATION_TYPES.MicroorganismGateway)
        private microorganismGateway: EntityGateway<Microorganism>,
        @inject(APPLICATION_TYPES.MatrixGateway)
        private matrixGateway: EntityGateway<Matrix>,
        @inject(APPLICATION_TYPES.SamplingReasonGateway)
        private samplingReasonGateway: EntityGateway<SamplingContext>
    ) {}

    private filterDefinitions: FilterDefinition[] = [
        {
            valueProvider: () =>
                this.microorganismGateway
                    .findAll()
                    .then(ary => ary.map(p => p.name)),
            id: 'erreger'
        },
        {
            valueProvider: () =>
                this.samplingReasonGateway
                    .findAll()
                    .then(ary => ary.map(p => p.name)),
            id: 'probenahmegrund'
        },
        {
            valueProvider: () =>
                this.matrixGateway.findAll().then(ary => ary.map(p => p.name)),
            id: 'matrix'
        }
    ];

    async getFilterConfiguration() {
        const filterConfiguration: FilterConfiguration[] = [];
        for (let index = 0; index < this.filterDefinitions.length; index++) {
            const configuration = await this.fromDefinitionToConfiguration(
                this.filterDefinitions[index]
            );
            filterConfiguration.push(configuration);
        }
        return filterConfiguration;
    }
    private async fromDefinitionToConfiguration(
        definition: FilterDefinition
    ): Promise<FilterConfiguration> {
        const values = await definition.valueProvider();
        return {
            id: definition.id,
            name: definition.id,
            values
        };
    }
}