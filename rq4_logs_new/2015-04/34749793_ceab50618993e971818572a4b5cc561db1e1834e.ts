/*
 *
 *          █████╗ ██╗     ██╗    ██╗ █████╗ ██╗   ██╗███████╗ ██████╗ ███╗   ██╗      ██╗██████╗  ██████╗
 *         ██╔══██╗██║     ██║    ██║██╔══██╗╚██╗ ██╔╝██╔════╝██╔═══██╗████╗  ██║██╗██╗██║██╔══██╗██╔════╝
 *         ███████║██║     ██║ █╗ ██║███████║ ╚████╔╝ ███████╗██║   ██║██╔██╗ ██║╚═╝╚═╝██║██████╔╝██║
 *         ██╔══██║██║     ██║███╗██║██╔══██║  ╚██╔╝  ╚════██║██║   ██║██║╚██╗██║██╗██╗██║██╔══██╗██║
 *         ██║  ██║███████╗╚███╔███╔╝██║  ██║   ██║   ███████║╚██████╔╝██║ ╚████║╚═╝╚═╝██║██║  ██║╚██████╗
 *         ╚═╝  ╚═╝╚══════╝ ╚══╝╚══╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝ ╚═════╝ ╚═╝  ╚═══╝      ╚═╝╚═╝  ╚═╝ ╚═════╝
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Michael Hohl
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import irc = require("../index");
import IrcSocketFactory = irc.IrcSocketFactory;

// 1. create a new factory
var factory = new IrcSocketFactory();
factory.on("ready", (id) => {
    console.log("ready[" + id + "]");
    setTimeout(() => {
        var ircSocket = factory.sockets["test"];
        ircSocket.raw(["JOIN", "##testflight"]);
        setTimeout(() => {
            var ircSocket = factory.sockets["test"];
            ircSocket.raw(["PRIVMSG", "##testflight", ":Hello World"]);
        }, 10000);
    }, 5000);
});
factory.on("data", (id, message) => {
    console.log("message[" + id + "]: " + message);
});

// 2. connect to some sample server
factory.createSocket("test", {
    server: "irc.freenode.net",
    port: 6667,
    nicknames: ["ISFTest", "SocketFactory"],
    username: "ircsocket123",
    realname: "Sample Bot"
});