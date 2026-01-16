import { afterAll, beforeAll, beforeEach, describe, expect, it } from '@jest/globals';
import { addSong, getSong } from '../src/helpers/playlist';
import { dequeue } from '../src/helpers/message_queue';
import { newClient, redisClient } from '../src/helpers/redis';

const REDIS_URL = process.env['REDIS_URL'] || 'redis://redis.service.consul:6379';
const CHANNEL_ID = 'test-channel-lol';
const QUEUE_KEY = `discord:channel:${CHANNEL_ID}:queue`
const testVideos = [
  {
    youtubeVideoTitle: "Around the World",
    youtubeVideoId: "Jb6gcoR266U",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "One More Time",
    youtubeVideoId: "fa5IWHDbftI",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Open Mic\\\\Aquarius III",
    youtubeVideoId: "NzkWWRa98g0",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Gray Area",
    youtubeVideoId: "qlUga4YnIvw",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "It Runs Through Me",
    youtubeVideoId: "QU_CoZvbLE4",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Feed The Fire",
    youtubeVideoId: "0TK9eyoxPc4",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Post Malone - Psycho ft. Ty Dolla $ign",
    youtubeVideoId: "au2n7VVGv_c",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Redbone",
    youtubeVideoId: "H_HkRMOwwGo",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Tints (feat. Kendrick Lamar)",
    youtubeVideoId: "YM1fyrMjjck",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "It Might Be Time",
    youtubeVideoId: "F9TiuqPXAoM",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Fade Away",
    youtubeVideoId: "b4qyMIOIDd4",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Get Lucky (feat. Pharrell Williams and Nile Rodgers)",
    youtubeVideoId: "4D7u5KF7SP8",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Lost In Translation",
    youtubeVideoId: "7ArAf-tETK0",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Starman (2012 Remaster)",
    youtubeVideoId: "aBKEt3MhNMM",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Machu Picchu",
    youtubeVideoId: "Xt_F4J4O-xo",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Under Cover of Darkness",
    youtubeVideoId: "Ho86D-yuBUw",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Voyager",
    youtubeVideoId: "OWiVJMgms9E",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Topanga",
    youtubeVideoId: "JumMelH3kI8",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Make Her Say",
    youtubeVideoId: "xcvO1uPD4VA",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Irene",
    youtubeVideoId: "rOokXli4250",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "I'm a Man",
    youtubeVideoId: "bj4bu1Bbbnw",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Trapdoor",
    youtubeVideoId: "fxYyslu7cDo",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Little Black Submarines",
    youtubeVideoId: "DhKAh4RJM0Q",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Tighten Up",
    youtubeVideoId: "xfSskvyxYGo",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Let It Happen",
    youtubeVideoId: "NMRhx71bGo4",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Borderline",
    youtubeVideoId: "rymYToIEL9o",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "The Less I Know The Better",
    youtubeVideoId: "PvM79DJ2PmM",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "The Race",
    youtubeVideoId: "1oyV8io1WOw",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Spice Girl",
    youtubeVideoId: "L26FaCh7YNA",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Come Home (feat. André 3000)",
    youtubeVideoId: "hEdBe04dwms",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "i",
    youtubeVideoId: "z5vHSXXZRDo",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Say It Ain't So",
    youtubeVideoId: "LQcMOI8dMas",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "My Name Is Jonas",
    youtubeVideoId: "wxlfkFMjLZc",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "LOYALTY.",
    youtubeVideoId: "J9JFXTENxvo",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Someday",
    youtubeVideoId: "eArVJFjd6S0",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Who Needs You",
    youtubeVideoId: "49F6zFu2B5c",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Santeria",
    youtubeVideoId: "FDdkpmSNo4U",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "What I Got",
    youtubeVideoId: "QtbxWas-3oM",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Walkin' On The Sun",
    youtubeVideoId: "GrrygZHK4z4",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Rawnald Gregory Erickson the Second",
    youtubeVideoId: "Du8viWKWEvM",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "No Waves",
    youtubeVideoId: "jQdY0Vb137c",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "The Distance",
    youtubeVideoId: "JH6il8U3EU0",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Sympathy For The Devil",
    youtubeVideoId: "HOYLkf3lZQo",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Mr. Blue Sky",
    youtubeVideoId: "bJ8Sz8CJY5g",
    interactionId: "123456789"
  },
  {
    youtubeVideoTitle: "Spiderwebs",
    youtubeVideoId: "UBVEie8bNdE",
    interactionId: "123456789"
  }
]

const testCaseCounts = [1, 3, undefined];


describe('Test playlist helper functions', () => {
  beforeAll(async () => await newClient(REDIS_URL));
  beforeEach(async () => await dequeue(QUEUE_KEY, -1));
  afterAll(async () => {
    await dequeue(QUEUE_KEY, -1);
    redisClient?.disconnect();
  });

  it.each(testCaseCounts)('should add %i songs to the playlist and retrieve them (NaN = all)', async (count) => {
    count = count || testVideos.length;
    let responses = [];
    await addSong(CHANNEL_ID, testVideos.slice(0, count));
    
    let song;
    while(song = await getSong(CHANNEL_ID, -1)) {
      responses.push(song)
    }

    expect(responses.length).toBe(count);
    expect(responses).toEqual(testVideos.slice(0, count));
    expect(await getSong(CHANNEL_ID, -1)).toBeUndefined();
  }, 10000);

  it.each(testCaseCounts)('should block and then retrieve %i songs when they are added to the playlist', async (count) => {
    count = count || testVideos.length;
    let responses = [];

    setTimeout(async () => {
      await addSong(CHANNEL_ID, testVideos.slice(0, count));
    }, 2000);

    let song;
    while(song = await getSong(CHANNEL_ID, 4)) { 
      responses.push(song); 
    }

    expect(responses.length).toBe(count);
    expect(responses).toEqual(testVideos.slice(0, count));
    expect(await getSong(CHANNEL_ID, -1)).toBeUndefined();
  }, 10000);

  it.each(testCaseCounts)('should block and timeout, returning nothing', async (count) => {
    count = count || testVideos.length;
    let responses = [];

    let song;
    while(song = await getSong(CHANNEL_ID, 4)) { 
      responses.push(song); 
    }

    expect(responses.length).toBe(0);
    expect(await getSong(CHANNEL_ID, -1)).toBeUndefined();
  }, 10000);

});