
import { Map } from '../../shared/models/map';
import { WorldMapCreator } from '../plugins/map/worldmapcreator';

const worldmap = new WorldMapCreator();

// TODO make world map persist until it is forcibly reset (maybe store the seed?)

export class GameState {

  world: Map;

  constructor() {
    this.world = worldmap.exportPolygons();
  }
}