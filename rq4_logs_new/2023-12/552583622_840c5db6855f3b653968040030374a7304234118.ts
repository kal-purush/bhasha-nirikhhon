import { describe, expect, it } from '@jest/globals';
import * as yt from '../src/helpers/youtube';
import { accessSync, rmSync } from 'fs';

const YT_TOKEN = process.env['YT_TOKEN'] || '';
const testPlaylistQuery = 'Kanye - Late Registration'
const testVideoQuery = 'Infected Mushroom - Walking on the Moon'
const searchCases = [
  { count: 5, type: 'video', query: testVideoQuery }, 
  { count: 5, type: 'playlist', query: testPlaylistQuery},
  { count: 51, type: 'video', query: testVideoQuery},
  { count: 51, type: 'playlist', query: testPlaylistQuery},
]

const playlistCases = [
  { 
    id: 'OLAK5uy_ljZc-x7bxToE4yodl2ujs7w0pBU9tyqKc', 
    videos: [
      { name: 'Wake Up Mr. West', id: 'Bwyu-SZ7g_E', type: 'video' },
      { name: "Heard 'Em Say", id: 'R6dH8iBHzb4', type: 'video' },
      { name: 'Touch The Sky', id: 'B95OUKk7alM', type: 'video' },
      { name: 'Gold Digger', id: 'uVL4d8P44eM', type: 'video' },
      { name: 'Skit #1', id: 'G4qTNRbAp-c', type: 'video' },
      { name: 'Drive Slow', id: 'Q1ViJEYNki4', type: 'video' },
      { name: 'My Way Home', id: 'TgAomHGqKUM', type: 'video' },
      { name: 'Crack Music', id: '2tmPSK-w90o', type: 'video' },
      { name: 'Roses', id: 'Qxlnb1lEdEs', type: 'video' },
      { name: 'Bring Me Down', id: 'CZ_-O31R3p4', type: 'video' },
      { name: 'Addiction', id: 'YuCwP-NbY0s', type: 'video' },
      { name: 'Skit #2', id: 'vRBOIbTyTnU', type: 'video' },
      { name: 'Diamonds From Sierra Leone (Remix)', id: '4q7OpvvfjWs', type: 'video' },
      { name: 'We Major', id: '_fr4SV4fGAw', type: 'video' },
      { name: 'Skit #3', id: 'HyXEzp85RGE', type: 'video' },
      { name: 'Hey Mama', id: 'B3NmMKfl3Ic', type: 'video' },
      { name: 'Celebration', id: 'FZjlP-N7Hl4', type: 'video' },
      { name: 'Skit #4', id: 'Y4r6lS04RpQ', type: 'video' },
      { name: 'Gone', id: 'TwPCaWQIJME', type: 'video' },
      {
        name: 'Diamonds From Sierra Leone (Bonus Track)',
        id: 'glTZy-Sujuw',
        type: 'video'
      },
      { name: 'Late', id: 'YRwTaWWK3dI', type: 'video' }
    ]  
  },
  {
    id: 'PLyvg7zitM7V3AqP9-pMsUGx8LZSMK-TDp',
    videos: [
      {
        "name": "Around the World",
        "id": "Jb6gcoR266U",
        "type": "video"
      },
      {
        "name": "One More Time",
        "id": "fa5IWHDbftI",
        "type": "video"
      },
      {
        "name": "Open Mic\\\\Aquarius III",
        "id": "NzkWWRa98g0",
        "type": "video"
      },
      {
        "name": "Gray Area",
        "id": "qlUga4YnIvw",
        "type": "video"
      },
      {
        "name": "It Runs Through Me",
        "id": "QU_CoZvbLE4",
        "type": "video"
      },
      {
        "name": "Feed The Fire",
        "id": "0TK9eyoxPc4",
        "type": "video"
      },
      {
        "name": "Post Malone - Psycho ft. Ty Dolla $ign",
        "id": "au2n7VVGv_c",
        "type": "video"
      },
      {
        "name": "Redbone",
        "id": "H_HkRMOwwGo",
        "type": "video"
      },
      {
        "name": "Tints (feat. Kendrick Lamar)",
        "id": "YM1fyrMjjck",
        "type": "video"
      },
      {
        "name": "It Might Be Time",
        "id": "F9TiuqPXAoM",
        "type": "video"
      },
      {
        "name": "Fade Away",
        "id": "b4qyMIOIDd4",
        "type": "video"
      },
      {
        "name": "Get Lucky (feat. Pharrell Williams and Nile Rodgers)",
        "id": "4D7u5KF7SP8",
        "type": "video"
      },
      {
        "name": "Lost In Translation",
        "id": "7ArAf-tETK0",
        "type": "video"
      },
      {
        "name": "Starman (2012 Remaster)",
        "id": "aBKEt3MhNMM",
        "type": "video"
      },
      {
        "name": "Machu Picchu",
        "id": "Xt_F4J4O-xo",
        "type": "video"
      },
      {
        "name": "Under Cover of Darkness",
        "id": "Ho86D-yuBUw",
        "type": "video"
      },
      {
        "name": "Voyager",
        "id": "OWiVJMgms9E",
        "type": "video"
      },
      {
        "name": "Topanga",
        "id": "JumMelH3kI8",
        "type": "video"
      },
      {
        "name": "Make Her Say",
        "id": "xcvO1uPD4VA",
        "type": "video"
      },
      {
        "name": "Irene",
        "id": "rOokXli4250",
        "type": "video"
      },
      {
        "name": "I'm a Man",
        "id": "bj4bu1Bbbnw",
        "type": "video"
      },
      {
        "name": "Trapdoor",
        "id": "fxYyslu7cDo",
        "type": "video"
      },
      {
        "name": "Little Black Submarines",
        "id": "DhKAh4RJM0Q",
        "type": "video"
      },
      {
        "name": "Tighten Up",
        "id": "xfSskvyxYGo",
        "type": "video"
      },
      {
        "name": "Let It Happen",
        "id": "NMRhx71bGo4",
        "type": "video"
      },
      {
        "name": "Borderline",
        "id": "rymYToIEL9o",
        "type": "video"
      },
      {
        "name": "The Less I Know The Better",
        "id": "PvM79DJ2PmM",
        "type": "video"
      },
      {
        "name": "The Race",
        "id": "1oyV8io1WOw",
        "type": "video"
      },
      {
        "name": "Spice Girl",
        "id": "L26FaCh7YNA",
        "type": "video"
      },
      {
        "name": "Come Home (feat. André 3000)",
        "id": "hEdBe04dwms",
        "type": "video"
      },
      {
        "name": "i",
        "id": "z5vHSXXZRDo",
        "type": "video"
      },
      {
        "name": "Say It Ain't So",
        "id": "LQcMOI8dMas",
        "type": "video"
      },
      {
        "name": "My Name Is Jonas",
        "id": "wxlfkFMjLZc",
        "type": "video"
      },
      {
        "name": "LOYALTY.",
        "id": "J9JFXTENxvo",
        "type": "video"
      },
      {
        "name": "Someday",
        "id": "eArVJFjd6S0",
        "type": "video"
      },
      {
        "name": "Who Needs You",
        "id": "49F6zFu2B5c",
        "type": "video"
      },
      {
        "name": "Santeria",
        "id": "FDdkpmSNo4U",
        "type": "video"
      },
      {
        "name": "What I Got",
        "id": "QtbxWas-3oM",
        "type": "video"
      },
      {
        "name": "Walkin' On The Sun",
        "id": "GrrygZHK4z4",
        "type": "video"
      },
      {
        "name": "Rawnald Gregory Erickson the Second",
        "id": "Du8viWKWEvM",
        "type": "video"
      },
      {
        "name": "No Waves",
        "id": "jQdY0Vb137c",
        "type": "video"
      },
      {
        "name": "The Distance",
        "id": "JH6il8U3EU0",
        "type": "video"
      },
      {
        "name": "Sympathy For The Devil",
        "id": "HOYLkf3lZQo",
        "type": "video"
      },
      {
        "name": "Mr. Blue Sky",
        "id": "bJ8Sz8CJY5g",
        "type": "video"
      },
      {
        "name": "Spiderwebs",
        "id": "UBVEie8bNdE",
        "type": "video"
      }
    ]
  }
]

const listCases = [
  {
    ids: playlistCases[0].videos.map(vid => vid.id),
    list: playlistCases[0].videos,
    type: 'video'
  },
  {
    ids: playlistCases[1].videos.map(vid => vid.id),
    list: playlistCases[1].videos,
    type: 'video'
  }
]

const downloadCases = [
  { path: './testdl1.mp3', id: 'Bwyu-SZ7g_E' },
  { path: undefined, id: 'R6dH8iBHzb4' }
]


describe('Youtube Helper Tests', () => {
  it.each(searchCases)('searches for $count $type and returns $count results', async({count, type, query}) => {
    const results = await yt.search(query, count, type as 'video' | 'playlist', YT_TOKEN);
    expect(results.length).toBe(count);
    results.forEach(result => {
      expect(result.type).toBe(type);
      expect(result.name).toBeDefined();
      expect(result.id).toBeDefined();
    })
  })

  it.each(playlistCases)('convert a playlist id $id to YTSearchResult[]', async ({id, videos}) => {
    let results = await yt.playlistToVideos(id, YT_TOKEN);
    expect(results).toEqual(videos);
  })

  it.each(listCases)('return YTSearchResult[] of a list of $type ids', async ({ids, list, type}) => {
    let results = await yt.list(ids, type as 'video' | 'playlist', YT_TOKEN);
    expect(results).toEqual(list);
  })

  it.each(downloadCases)('download/stream id $id and return read stream', async ({ id, path }) => {
    let stream = await yt.download(id, path);
    expect(stream.readable).toBe(true);
    if(path) {
      accessSync(path);
      rmSync(path);
    } 
  })
});


