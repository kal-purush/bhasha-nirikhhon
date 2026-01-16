import TimeRange from './physics/time-range/time-range';
import Player from './player';

import Bonus from './item-activation/bonus/bonus';
import Penalty from './item-activation/penalty/penalty';
import Map from './map';

export default interface Game {
  /** Unique ID. */
  id: number;

  /** Time related. */
  range: TimeRange<Date>;

  /** Players. */
  players: Player[];

  /** Owner of the game. */
  owner: Player;

  /** Map players are playing on. */
  map: Map;

  /** Different bonus that may appear. */
  bonus: Bonus;

  /** Different penalties that may appear. */
  penalty: Penalty;
}