import fs = require('fs');
import Queue = require('./Queue');
import Song = require('./Song');
import Fetch = require('./Fetch');

var bot: any;
var queue: Queue;

// Main loop
function Play(dqueue: Queue, dbot: any): void {
    bot = dbot;
    queue = dqueue;
    
    setInterval(() => {
        queue.get({ playing: false }, (docs: any) => {
            if (docs.length > 0) Stream();
        });
    }, 1000);
}

function Stream(): void {
    queue.first((doc: any) => {
        var song: Song = doc[0];
        Fetch.download(song, (file: any) => {
            console.log("Playing '" + song.title + "'...");
            bot.voiceConnection.playFile(file);
        });
        queue.remove(doc[0]);
    });
}

export = Play;