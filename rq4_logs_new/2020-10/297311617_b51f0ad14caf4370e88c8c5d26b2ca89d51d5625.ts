import { blockingDistance } from "../../src/level/blockingdistance"

describe("function takes in a location and direction", function() {
  it("returns 0 if no path is in that direction", function() {
    var levelMap = [{
      "position": {"x": 0, "y": 0},
      "paths": { "North": false, "South": false, "East": false, "West": false},
      "openPaths": {"North": false, "South": false, "East": false, "West": false},
      "numberOpenPaths": 0,
      "exitTile": false
    }]
    expect(blockingDistance(levelMap, {x:0, y:0}, "North")).toEqual(0)
    expect(blockingDistance(levelMap, {x:0, y:0}, "South")).toEqual(0)
    expect(blockingDistance(levelMap, {x:0, y:0}, "East")).toEqual(0)
    expect(blockingDistance(levelMap, {x:0, y:0}, "West")).toEqual(0)
  })

  it("returns 1 if one path is in that direction", function() {
    var levelMap = [{
      "position": {"x": 0, "y": 0},
      "paths": { "North": true, "South": false, "East": false, "West": false},
      "openPaths": {"North": false, "South": false, "East": false, "West": false},
      "numberOpenPaths": 1,
      "exitTile": false
    },
    {
      "position": {"x": 0, "y": 1},
      "paths": { "North": false, "South": true, "East": false, "West": false},
      "openPaths": {"North": false, "South": false, "East": false, "West": false},
      "numberOpenPaths": 0,
      "exitTile": false
    }]
    expect(blockingDistance(levelMap, {x:0, y:0}, "North")).toEqual(1)
    expect(blockingDistance(levelMap, {x:0, y:0}, "South")).toEqual(0)
    expect(blockingDistance(levelMap, {x:0, y:0}, "East")).toEqual(0)
    expect(blockingDistance(levelMap, {x:0, y:0}, "West")).toEqual(0)
  })
})