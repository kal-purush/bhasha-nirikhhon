// Description:
//  Mumble channel announcements and youtube player
//
// Commands:
//  hubot who <channel> - return all users currently in the given channel
//
// Author:
//  Matt Perry

/// <reference path="..\..\typings\main.d.ts" />

import { ConnectionOptions } from "tls";
import { Robot } from "tsbot";
import * as Queue from "promise-queue";
import * as youtubedl from "youtube-dl";
import * as ffmpeg from "fluent-ffmpeg";
import * as mumble from "mumble";
import * as thenify from "thenify";
import * as fs from "fs";

class Player {
    private _cmd: FfmpegCommand;
    private _output: mumble.MumbleInputStream;
    private _playing: boolean;
    private _gain: number;
    private _queue: Queue;

    constructor(private _cli: mumble.MumbleClient) {
        this._playing = false;
        this._gain = .1;
        this._queue = new Queue(1);
    }

    public add(url: string): void {
        this._queue.add(this._stream.bind(this, url));
    }

    public pause(): void {
        this._switchState(false, "SIGSTOP");
    }

    public resume(): void {
        this._switchState(true, "SIGCONT");
    }

    public skip(): void {
        this._switchState(false, "SIGKILL");
    }

    public clear(): void {
        this._queue.queue = [];
        this.skip();
    }

    public volume(vol: number): void {
        let gain = vol * .01;
        if (this._playing && gain > 0 && gain <= 1) {
            this._gain = gain;
            this._output.setGain(this._gain);
        }
    }

    private _switchState(newState: boolean, signal: string): void {
        if (this._playing !== newState) {
            this._cmd.kill(signal);
            this._playing = newState;
        }
    }

    private async _stream(url: string): Promise<any> {
        let info = await thenify<string, string[], VideoInfo>(youtubedl.getInfo)(url, []);
        this._output = this._cli.inputStream({ gain: this._gain });
        this._cmd = ffmpeg(info.url)
            .noVideo()
            .format("s16le")
            .audioBitrate(128)
            .audioChannels(1)
            .audioFrequency(48000)
            .on("start", () => this._playing = true)
            .on("error", () => this._playing = false)
            .on("end", () => this._playing = false);
        this._cmd.pipe(this._output);

        return new Promise((resolve, reject) => {
            this._cmd.on("error", reject);
            this._cmd.on("end", resolve);
        });
    }
}

class MumbleBot {
    private _name: string;
    private _timeMap: { [user: string]: number };
    private _player: Player;

    constructor(private _robot: Robot) {
        this._name = "DonkeyBot";
        this._timeMap = {};
    }

    public async connect(url: string, password: string): Promise<void> {
        try {
            let options = await this._getOptions();
            let cli = await thenify<string, ConnectionOptions, mumble.MumbleClient>(mumble.connect)(url, options);
            cli.authenticate(this._name, password);

            this._player = new Player(cli);
            cli.on("ready", () => this._ready(cli));
            cli.on("user-move", this._userMove.bind(this));
            cli.on("message", (msg) => {
                this._message(msg.replace(/<[^>]+>/ig, ""));
            });
            this._addResponder(cli);
        } catch (err) {
            this._robot.logger.error(err);
        }
    }

    private async _getOptions(): Promise<ConnectionOptions> {
        let opts: ConnectionOptions = {};
        if (fs.existsSync("./certs")) {
            opts.key = await thenify(fs.readFile)("./certs/private_key.pem");
            opts.cert = await thenify(fs.readFile)("./certs/cert.pem");
        }
        return opts;
    }

    private _ready(cli: mumble.MumbleClient): void {
        cli.user.moveToChannel("Games");
    }

    private _userMove(user: mumble.User, oldChannel: mumble.Channel, newChannel: mumble.Channel): void {
        if (newChannel.name === "Games" && user.name !== this._name) {
            let currentTime = Date.now();
            if (!this._timeMap[user.name] || (currentTime - this._timeMap[user.name]) > 60000) {
                this._timeMap[user.name] = currentTime;
                this._robot.adapter.send({}, `${user.name} wants to play games!`);
            }
        }
    }

    private _message(msg: string): void {
        let commands = [
            { regex: /^!p(ause)?$/, fn: () => this._player.pause() },
            { regex: /^!r(esume)?$/, fn: () => this._player.resume() },
            { regex: /^!s(kip)?$/, fn: () => this._player.skip() },
            { regex: /^!a(dd)? ([^\s]+)$/, fn: (match: RegExpMatchArray) => this._player.add(match[2]) },
            { regex: /^!c(lear)?$/, fn: () => this._player.clear() },
            { regex: /^!v(ol|olume)? ([^\s]+)$/, fn: (match: RegExpMatchArray) => this._player.volume(parseInt(match[2])) }
        ];

        for (let cmd of commands.filter((cmd) => cmd.regex.test(msg))) {
            cmd.fn(cmd.regex.exec(msg));
        }
    }

    private _addResponder(cli: mumble.MumbleClient): void {
        this._robot.respond(/who (.*)/i, (res) => {
            let channelName = res.match[1];
            let channel = cli.channelByName(channelName);
            if (channel != null) {
                let userNames = channel.users.map((user) => user.name);
                if (userNames.length > 0) {
                    res.send(...userNames);
                } else {
                    res.send(`Nobody is in ${channelName}`);
                }
            } else {
                res.send(`No channel with name ${channelName}`);
            }
        });
    }
}

export = async (robot: Robot) => {
    try {
        await (new MumbleBot(robot)).connect(process.env.HUBOT_MUMBLE_URL, process.env.HUBOT_MUMBLE_PASSWORD);
    } catch (e) {
        robot.logger.error(e);
    }
};