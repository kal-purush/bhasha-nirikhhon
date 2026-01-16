/* eslint camelcase: 0 */

export interface Coordinate {
  // Each coordinate has an 'x' and 'y'
  latitude: number
  longitude: number
}

export interface Player {
  // Each player has HP and a coordinate
  health: number
  location: Coordinate
}

export interface Zone {
  center: Coordinate // Coordinate for center of zone
  active_time: string // How long the zone will be active before disappearing
  radius_meters: number // Radius of the circular zone
}

export interface GameState {
  // Multiplayer game state
  players: {
    // List of players categorized by their id
    [id: string]: Player
  }
  zones: Zone[] // List of zones
}