import { Store } from './interface';
import { eCEventsDataSeed } from './seeds/ec-event-data';
import { environmentsSeed } from './seeds/environment';
import { humidityEventsDataSeed } from './seeds/humidity-event-data';
import { lightBulbTemplatesSeed } from './seeds/light-bulb-template';
import { lightEventsDataSeed } from './seeds/light-event-data';
import { environmentsLightBulbTemplatesSeed } from './seeds/many-to-many/environment-lightbulbtemplate';
import { eventsTargetsSeed } from './seeds/many-to-many/event-target';
import { speciesPlantStagesSeed } from './seeds/many-to-many/species-plantstage';
import { pHEventsDataSeed } from './seeds/ph-event-data';
import { photosSeed } from './seeds/photo';
import { plantsSeed } from './seeds/plant';
import { plantStagesSeed } from './seeds/plant-stage';
import { plantStageEventsDataSeed } from './seeds/plant-stage-event-data';
import { speciesSeed } from './seeds/species';
import { temperatureEventsDataSeed } from './seeds/temperature-event-data';
import { waterEventsDataSeed } from './seeds/water-event-data';

/**
 * In-Memory DB
 */
export const store: Store = {
  plants: plantsSeed,
  species: speciesSeed,
  photos: photosSeed,
  environments: environmentsSeed,
  plantStages: plantStagesSeed,
  lightBulbTemplates: lightBulbTemplatesSeed,
  // event data
  waterEventsData: waterEventsDataSeed,
  plantStageEventsData: plantStageEventsDataSeed,
  temperatureEventsData: temperatureEventsDataSeed,
  humidityEventsData: humidityEventsDataSeed,
  pHEventsData: pHEventsDataSeed,
  eCEventsData: eCEventsDataSeed,
  lightEventsData: lightEventsDataSeed,
  // relationships (many to many)
  speciesPlantStages: speciesPlantStagesSeed,
  environmentsLightBulbTemplates: environmentsLightBulbTemplatesSeed,
  eventsTargets: eventsTargetsSeed
};