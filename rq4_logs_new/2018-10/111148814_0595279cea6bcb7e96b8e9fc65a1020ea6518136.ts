import {Unit} from './unit/unit'
import {GameTileHex} from './board'
import {CubeCoordinates} from 'honeycomb-grid'

export interface UnitPosition {
  unit: Unit,
  position: CubeCoordinates
}

export class BoardState {
  constructor(public terrain: Array<GameTileHex> = [], public units: Array<UnitPosition> = []) {}

  updateUnit(unit: Unit, position: CubeCoordinates) {
    const units = this.units.slice()

    return new BoardState(this.terrain, units.map(u => {
      if (u.unit != unit) { return u }
      return {unit: unit, position: position}
    }))
  }

  addUnit(unit: Unit, position: CubeCoordinates) {
    const units = this.units.slice().concat([{unit: unit, position: position}])
    return new BoardState(this.terrain, units)
  }
}