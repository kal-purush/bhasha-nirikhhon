import SpotifyWrapper from '../src/index'

global.fetch = require('node-fetch')

const spotify = new SpotifyWrapper({
  token: 'BQCjlNC3x8li4BH0oLP8nfQ61qzijh6eA-dTu1aQyYh4X0vgcujP7weNMuIpQrGX_3_1yTpu2zWnlBa_GkEji0aleR-t5_OEv-IyUQqO8bqXfuiJk-g6iZ-VZKgGQAYR2_ptPkXbCcVObZKT'
})

let albums = spotify.search.albums('Shontelle')
albums.then(data => data.albums.items.map(item => console.log(item.name)))