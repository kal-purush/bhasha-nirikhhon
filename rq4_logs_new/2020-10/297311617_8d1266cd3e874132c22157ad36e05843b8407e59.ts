import { Position, Tile, directionValues } from "../helpers/typesAndConst"
import { Level } from "./level"
import { tileExistsAt, tileExistsAtOffset } from "../helpers/helpers"

var i:number;
var j:number;

export class LevelCreator {

  createLevel(number): Level {
    var levelMap: Tile[] = [ {
      position: {"x": 0, "y": 0},
      paths: {"North": true, "South": true, "East": false, "West": false},
      openPaths: {"North": true, "South": true, "East": false, "West": false},
      numberOpenPaths: 2,
      exitTile: false
    } ]
    for ( i = 0; i < number**2 - 1; i++ ) {
      if ( i === levelMap.length -1 && levelMap[i].numberOpenPaths === 0) {
        this.makeOneOpenPath(levelMap[i], levelMap)
      }
      while (levelMap[i].numberOpenPaths > 0 ) {
        levelMap.push(this.createSingleTile(levelMap[i], levelMap))
      }
    }
    return new Level(this.tidy(levelMap))
  }

  makeOneOpenPath(tile:Tile, levelMap:Tile[]) {
    for (let direction in tile.openPaths) {
      tile.paths[direction] = true
      tile.openPaths[direction] = true
      
      for ( j = 0; j < levelMap.length; j++ ) {
        if (levelMap[j].position.x + directionValues[direction].x === tile.position.x
            && levelMap[j].position.y + directionValues[direction].y === tile.position.y) {
          tile.paths[direction] = false
          tile.openPaths[direction] = false
        }
      }
      if (tile.paths[direction] === true) {
        tile.numberOpenPaths ++
        break
      }
    }
  }

  createSingleTile(previousTile:Tile, levelMap:Tile[]) {
    var singleTile:Tile = {
      position: {"x": 0, "y": 0},
      paths: {"North": false, "South": false, "East": false, "West": false},
      openPaths: {"North": false, "South": false, "East": false, "West": false},
      numberOpenPaths: 0,
      exitTile: false
    }
    var openPathValue: { x:number, y:number } = {x: 0, y: 0}

    for (let direction in previousTile.openPaths) {
      if (previousTile.openPaths[direction]) {
        openPathValue.x = directionValues[direction].x
        openPathValue.y = directionValues[direction].y
        singleTile.paths[directionValues[direction].opposite] = true
        
        previousTile.openPaths[direction] = false
        previousTile.numberOpenPaths--
      
        break
      }
    }

    singleTile.position.x = previousTile.position.x + openPathValue.x
    singleTile.position.y = previousTile.position.y + openPathValue.y

    this.checkForNearbyTiles(singleTile, levelMap)
    
    this.createOpenPaths(singleTile)

    return singleTile
  }

  checkForNearbyTiles(tile:Tile, levelMap:Tile[]) {
    for (let direction in tile.openPaths) {
      for ( j = 0; j < levelMap.length; j++ ) {
        if (levelMap[j].position.x + directionValues[direction].x === tile.position.x
            && levelMap[j].position.y + directionValues[direction].y === tile.position.y) {
          tile.paths[directionValues[direction].opposite] = levelMap[j].paths[direction]
        }
      }
    }
  }

  createOpenPaths(tile:Tile) {
    for( let direction in tile.paths) {
      if (tile.paths[direction] === false && Math.random() > 0.2){
        tile.paths[direction] = true
        tile.openPaths[direction] = true
        tile.numberOpenPaths ++
      }
    }
  }

  tidy(levelMap:Tile[]) {
    for ( i = 0; i < levelMap.length; i++ ) {
      for( let direction in levelMap[i].paths) {
        if (levelMap[i].openPaths[direction] === true){
          levelMap[i].paths[direction] = false
          levelMap[i].openPaths[direction] = false
          levelMap[i].numberOpenPaths --
        }
      }
    }
    levelMap[levelMap.length-1].exitTile = true
    return levelMap
  }
}