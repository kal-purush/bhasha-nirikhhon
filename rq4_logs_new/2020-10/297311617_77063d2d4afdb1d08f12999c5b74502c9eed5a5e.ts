import { Position, Tile, directionValues } from "./typesAndConst"

export function findCorrectTile(levelMap: Tile[], location: Position): Tile {
  var output
  levelMap.forEach(function(tile) {
    if (tile.position.x == location.x && tile.position.y == location.y) {
      output = tile
    }
  })
  return output
}

export function tileExistsAt(levelMap: Tile[], location: Position): Boolean {
  var test = findCorrectTile(levelMap, location)
  return (!!test)
}

export function tileExistsAtOffset(levelMap: Tile[], location: Position, givenDirection: String): Boolean {
  for (let direction in directionValues){
    if (givenDirection === direction.toString()){
      location.x  += directionValues[direction].x
      location.y  += directionValues[direction].y
    }
  }
  return tileExistsAt(levelMap, location)
}