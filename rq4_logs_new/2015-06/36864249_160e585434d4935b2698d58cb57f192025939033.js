/**
 * Copyright 2015, Nominet UK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


var getdns = require('getdns');


module.exports = function (RED) {
    "use strict";

    function DnssecQueryNode(config) {

        RED.nodes.createNode(this, config);
        var node = this;
        this.on("input", function (msg) {


            var req = null;

            msg.dnsResponse = {};

            if (msg.dnsQuery.name && msg.dnsQuery.type) {


                var timeout = 5000;

                if (msg.dnsQuery.timeout) timeout = msg.dnsQuery.timeout;

                var options = {
                    // option for stub resolver context
                    stub: false,		// for some reason, stub resolution not working on rPi as configured

                    // upstream recursive servers
                    //  upstreams : [ "8.8.8.8" ],

                    // request timeout time in millis
                    timeout: timeout,

                    // always return dnssec status
                    return_dnssec_status: true
                };

                // create the context with the above options
                var context = getdns.createContext(options);


                var requested_name = msg.dnsQuery.name, ;

                var requested_rrtype = null;

                //TODO: to be completed
                switch (msg.dnsQuery.type) {
                    case "URI":
                        requested_rrtype = getdns.RRTYPE_URI;
                        break;
                        ccase
                        "A"
                    :
                        requested_rrtype = getdns.RRTYPE_A;
                        break;
                    case "AAAA":
                        requested_rrtype = getdns.RRTYPE_AAAA;
                        break;
                    case "CNAME":
                        requested_rrtype = getdns.RRTYPE_CNAME;
                        break;
                    case "TXT":
                        requested_rrtype = getdns.RRTYPE_TXT;
                        break;

                }


                context.lookup(requested_name, requested_rrtype, function (err, result) {
                    // if not null, err is an object w/ msg and code.
                    // code maps to a GETDNS_CALLBACK_TYPE
                    // result is a response dictionary
                    // A third argument is also supplied as the transaction id
                    // See below for the format of response

                    if (err !== null) {
                        console.log("callback: err");
                        console.log(err)

                        msg.dnsResponse.type = "Error";
                        msg.dnsResponse.value = "Lookup error";
                        node.send(msg);
                        context.destroy();
                    }
                    else {
                        console.log("callback: result");

                        // expecting a single reply, although that may contain multiple records
                        if (result.replies_tree.length != 1) {
                            msg.dnsResponse.type = "Error";
                            msg.dnsResponse.value = "expected single reply to URI query";
                            node.send(msg);
                            context.destroy();
                        }
                        var reply = result.replies_tree[0];

                        // check DNSSEC
                        if (reply.dnssec_status != getdns.DNSSEC_SECURE) {
                            msg.dnsResponse.type = "Error";
                            msg.dnsResponse.value = "DNSSEC failed";
                            node.send(msg);
                            context.destroy();

                        }


                        // enumerate output
                        for (var i = 0; i < reply.answer.length; i++) {
                            var a = reply.answer[i];
                            if (a.type == getdns.RRTYPE_URI) {
                                console.log("reply: URI record: " + a.rdata.priority + " " + a.rdata.weight + " \"" + a.rdata.target + "\"");
                                msg.dnsResponse.type = "URI";
                                msg.dnsResponse.value = a.rdata.target;
                                node.send(msg);
                            } else if (a.type == getdns.RRTYPE_RRSIG) {
                                console.log("reply: RRSIG record");
                            } else {
                                throw new Error("unexpected record type");
                            }
                        }

                        context.destroy();

                    }
                });


            }
        });
    }

    RED.nodes.registerType("dnssec-query", DnssecQueryNode);
}