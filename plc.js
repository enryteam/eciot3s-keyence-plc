/**
 * Copyright JS Foundation and other contributors, http://js.foundation
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
module.exports = function(RED) {
    "use strict";
    //////////////////////////////////////////////////////////////////////////////////////////TCP
    var socketTimeout = RED.settings.socketTimeout || null;
    const msgQueueSize = RED.settings.tcpMsgQueueSize || 1000;
    const Denque = require('denque');
    var net = require('net');
    //Monitor all list state variables
    var Monitoring_data = [];
    var flag_status = [];
    var send_ok_flag = false;
    /**
     * Enqueue `item` in `queue`
     * @param {Denque} queue - Queue
     * @param {*} item - Item to enqueue
     * @private
     * @returns {Denque} `queue`
     */
    const enqueue = (queue, item) => {
        // drop msgs from front of queue if size is going to be exceeded
        if (queue.length === msgQueueSize) { queue.shift(); }
        queue.push(item);
        return queue;
    };
    /**
     * Shifts item off front of queue
     * @param {Deque} queue - Queue
     * @private
     * @returns {*} Item previously at front of queue
     */
    const dequeue = queue => queue.shift();
    ////////////////////////////////////////////////////////////////////////////////////////////////Switch
    var operators = {
        'eq': function(a, b) { return a == b; },
        'neq': function(a, b) { return a != b; },
        'lt': function(a, b) { return a < b; },
        'lte': function(a, b) { return a <= b; },
        'gt': function(a, b) { return a > b; },
        'gte': function(a, b) { return a >= b; },
        // 'btwn': function(a, b, c) { return (a >= b && a <= c) || (a <= b && a >= c); },
        // 'cont': function(a, b) { return (a + "").indexOf(b) != -1; },
        // 'regex': function(a, b, c, d) { return (a + "").match(new RegExp(b, d ? 'i' : '')); },
        // 'true': function(a) { return a === true; },
        // 'false': function(a) { return a === false; },
        // 'null': function(a) { return (typeof a == "undefined" || a === null); },
        // 'nnull': function(a) { return (typeof a != "undefined" && a !== null); },
        // 'empty': function(a) {
        //     if (typeof a === 'string' || Array.isArray(a) || Buffer.isBuffer(a)) {
        //         return a.length === 0;
        //     } else if (typeof a === 'object' && a !== null) {
        //         return Object.keys(a).length === 0;
        //     }
        //     return false;
        // },
        // 'nempty': function(a) {
        //     if (typeof a === 'string' || Array.isArray(a) || Buffer.isBuffer(a)) {
        //         return a.length !== 0;
        //     } else if (typeof a === 'object' && a !== null) {
        //         return Object.keys(a).length !== 0;
        //     }
        //     return false;
        // },
        // 'istype': function(a, b) {
        //     if (b === "array") { return Array.isArray(a); } else if (b === "buffer") { return Buffer.isBuffer(a); } else if (b === "json") {
        //         try { JSON.parse(a); return true; } // or maybe ??? a !== null; }
        //         catch (e) { return false; }
        //     } else if (b === "null") { return a === null; } else { return typeof a === b && !Array.isArray(a) && !Buffer.isBuffer(a) && a !== null; }
        // },
        // 'head': function(a, b, c, d, parts) {
        //     var count = Number(b);
        //     return (parts.index < count);
        // },
        // 'tail': function(a, b, c, d, parts) {
        //     var count = Number(b);
        //     return (parts.count - count <= parts.index);
        // },
        // 'index': function(a, b, c, d, parts) {
        //     var min = Number(b);
        //     var max = Number(c);
        //     var index = parts.index;
        //     return ((min <= index) && (index <= max));
        // },
        // 'hask': function(a, b) {
        //     return a !== undefined && a !== null && (typeof b !== "object") && a.hasOwnProperty(b + "");
        // },
        // 'jsonata_exp': function(a, b) { return (b === true); },
        // 'else': function(a) { return a === true; }
    };

    var _maxKeptCount;

    function getMaxKeptCount() {
        if (_maxKeptCount === undefined) {
            var name = "nodeMessageBufferMaxLength";
            if (RED.settings.hasOwnProperty(name)) {
                _maxKeptCount = RED.settings[name];
            } else {
                _maxKeptCount = 0;
            }
        }
        return _maxKeptCount;
    }
    //-------------------------------------------------------------------------------------------------------//
    //get attribute
    function getProperty(node, msg, done) {
        if (node.propertyType === 'jsonata') {
            RED.util.evaluateJSONataExpression(node.property, msg, (err, value) => {
                if (err) {
                    done(RED._("switch.errors.invalid-expr", { error: err.message }));
                } else {
                    done(undefined, value);
                }
            });
        } else {
            //msg
            RED.util.evaluateNodeProperty(node.property, node.propertyType, node, msg, (err, value) => {
                if (err) {
                    done(undefined, undefined);
                } else {
                    done(undefined, value);
                }
            });
        }
    }

    function getV1(node, msg, rule, hasParts, done) {
        if (rule.vt === 'prev') {
            return done(undefined, node.previousValue);
        } else if (rule.vt === 'jsonata') {
            var exp = rule.v;
            if (rule.t === 'jsonata_exp') {
                if (hasParts) {
                    exp.assign("I", msg.parts.index);
                    exp.assign("N", msg.parts.count);
                }
            }
            RED.util.evaluateJSONataExpression(exp, msg, (err, value) => {
                if (err) {
                    done(RED._("switch.errors.invalid-expr", { error: err.message }));
                } else {
                    done(undefined, value);
                }
            });
        } else if (rule.vt === 'json') {
            done(undefined, "json"); // TODO: ?! invalid case
        } else if (rule.vt === 'null') {
            done(undefined, "null");
        } else {
            RED.util.evaluateNodeProperty(rule.v, rule.vt, node, msg, function(err, value) {
                if (err) {
                    done(undefined, undefined);
                } else {
                    done(undefined, value);
                }
            });
        }
    }

    function getV2(node, msg, rule, done) {
        var v2 = rule.v2;
        if (rule.v2t === 'prev') {
            return done(undefined, node.previousValue);
        } else if (rule.v2t === 'jsonata') {
            RED.util.evaluateJSONataExpression(rule.v2, msg, (err, value) => {
                if (err) {
                    done(RED._("switch.errors.invalid-expr", { error: err.message }));
                } else {
                    done(undefined, value);
                }
            });
        } else if (typeof v2 !== 'undefined') {
            RED.util.evaluateNodeProperty(rule.v2, rule.v2t, node, msg, function(err, value) {
                if (err) {
                    done(undefined, undefined);
                } else {
                    done(undefined, value);
                }
            });
        } else {
            done(undefined, v2);
        }
    }
    //Use a single rule
    function applyRule(node, msg, property, state, done) {
        //state.currentRule++
        var rule = node.rules[state.currentRule];
        var v1, v2;
        getV1(node, msg, rule, state.hasParts, (err, value) => {
            if (err) {
                // This only happens if v1 is an invalid JSONata expr
                // But that will have already been logged and the node marked
                // invalid as part of the constructor
                return done(err);
            }
            v1 = value;
            getV2(node, msg, rule, (err, value) => {
                if (err) {
                    // This only happens if v1 is an invalid JSONata expr
                    // But that will have already been logged and the node marked
                    // invalid as part of the constructor
                    return done(err);
                }
                v2 = value;
                if (rule.t == "else") {
                    property = state.elseflag;
                    state.elseflag = true;
                }
                //Conditional rules applied to each row, selected based on selection
                for (var x = 0; x < msg.arr_length; x++) {
                    if (node.rules[state.currentRule].p == msg.payload[x].id) {
                        property = Number(msg.payload[x].val);
                    }
                }
                // console.log('number of calls' + state.currentRule);
                //Apply one or more
                try {
                    if (operators[rule.t](property, v1, v2, rule.case, msg.parts)) {

                        state.onward.push(msg);
                        state.elseflag = false;
                        if (node.checkall == "false") {
                            return done(undefined, false);
                        }
                    } else {
                        state.onward.push(null);
                    }
                    //  Output custom //  execute when all conditions are traversed
                    if (state.currentRule + 1 === node.rules.length) {
                        var num_onward = 0
                            // console.log(state.onward);
                        for (var i = 0; i < node.rules.length; i++) {
                            if (state.onward[i]) {
                                num_onward++;
                            }
                        }
                        //When the condition is greater than 1, the first one is not output and the timer stops
                        if (num_onward > 1) {
                            state.onward[0] = null;
                            //timing mark
                            send_ok_flag = true;
                            for (var m = 0; m < Monitoring_data.length; m++) {
                                //Find the starting address in the global
                                if (Monitoring_data[m].address == node.addr_rd && Monitoring_data[m].server == node.server && Monitoring_data[m].port == node.port) {
                                    for (var n = 0; n < node.num_rd; n++) {
                                        if (Monitoring_data[m + n].status == 1) {
                                            // Reset global flags, zero status 
                                            Monitoring_data[m + n].status = 0;
                                            // console.log(Monitoring_data);
                                        }
                                    }
                                }
                            }
                            // console.log(state.onward);
                        }
                    }
                    done(undefined, state.currentRule < node.rules.length - 1);
                } catch (err) {
                    // An error occurred evaluating the rule - for example, an
                    // invalid RegExp value.
                    done(err);
                }
            });
        });
    }
    // Use multiple rules
    function applyRules(node, msg, property, state, done) {
        if (!state) {
            if (node.rules.length === 0) {
                done(undefined, []);
                return;
            }
            state = {
                currentRule: 0,
                elseflag: true,
                onward: [],
                hasParts: msg.hasOwnProperty("parts") &&
                    msg.parts.hasOwnProperty("id") &&
                    msg.parts.hasOwnProperty("index")
            }
        }
        applyRule(node, msg, property, state, (err, hasMore) => {
            if (err) {
                return done(err);
            }
            // Multi-line
            if (hasMore) {
                state.currentRule++;
                //callback
                applyRules(node, msg, property, state, done);
            } else {
                node.previousValue = property;
                done(undefined, state.onward);
            }
        });
    }
    //------------------------------------------------------------------------------------------------//

    function SwitchNode(n) {
        RED.nodes.createNode(this, n);
        ////////////////////////////////////////////////tcp
        this.server = n.server;
        this.port = Number(n.port);
        this.out = n.out;
        this.splitc = n.splitc;
        this.model = n.model;
        this.wait_check = n.wait_check;
        this.interval_sw_time = n.interval_sw_time;
        this.addr_rd = Number(n.RD_Addr);
        this.num_rd = Number(n.RD_Num);
        this.addr_wr = Number(n.WR_Addr);
        this.num_wr = n.WR_Value;


        //return method
        //time-time delay
        //char-end of specified character
        //count-Specify receive length
        //sit-stay connected
        //immed-no need to wait
        if (this.out === "immed") {
            this.splitc = -1;
            this.out = "time";
        }
        if (this.out !== "char") { this.splitc = Number(this.splitc); } else {
            if (this.splitc[0] == '\\') {
                this.splitc = parseInt(this.splitc.replace("\\n", 0x0A).replace("\\r", 0x0D).replace("\\t", 0x09).replace("\\e", 0x1B).replace("\\f", 0x0C).replace("\\0", 0x00));
            } // jshint ignore:line
            if (typeof this.splitc == "string") {
                if (this.splitc.substr(0, 2) == "0x") {
                    this.splitc = parseInt(this.splitc);
                } else {
                    this.splitc = this.splitc.charCodeAt(0);
                }
            } // jshint ignore:line
        }

        var node = this;
        var clients = {};

        /////////////////////////////////////////////////tcp
        this.rules = n.rules || [];
        this.property = n.property;
        this.propertyType = n.propertyType || "msg";
        if (this.propertyType === 'jsonata') {
            try {
                this.property = RED.util.prepareJSONataExpression(this.property, this);
            } catch (err) {
                this.error(RED._("switch.errors.invalid-expr", { error: err.message }));
                return;
            }
        }

        this.checkall = n.checkall || "true";
        this.previousValue = null;
        var node = this;
        var valid = true;
        var repair = n.repair;
        var needsCount = repair;
        for (var i = 0; i < this.rules.length; i += 1) {
            var rule = this.rules[i];
            needsCount = needsCount || ((rule.t === "tail"));
            if (!rule.vt) {
                if (!isNaN(Number(rule.v))) {
                    rule.vt = 'num';
                } else {
                    rule.vt = 'str';
                }
            }
            if (rule.vt === 'num') {
                if (!isNaN(Number(rule.v))) {
                    rule.v = Number(rule.v);
                }
            } else if (rule.vt === "jsonata") {
                try {
                    rule.v = RED.util.prepareJSONataExpression(rule.v, node);
                } catch (err) {
                    this.error(RED._("switch.errors.invalid-expr", { error: err.message }));
                    valid = false;
                }
            }
            if (typeof rule.v2 !== 'undefined') {
                if (!rule.v2t) {
                    if (!isNaN(Number(rule.v2))) {
                        rule.v2t = 'num';
                    } else {
                        rule.v2t = 'str';
                    }
                }
                if (rule.v2t === 'num') {
                    rule.v2 = Number(rule.v2);
                } else if (rule.v2t === 'jsonata') {
                    try {
                        rule.v2 = RED.util.prepareJSONataExpression(rule.v2, node);
                    } catch (err) {
                        this.error(RED._("switch.errors.invalid-expr", { error: err.message }));
                        valid = false;
                    }
                }
            }
        }
        if (!valid) {
            return;
        }

        var pendingCount = 0;
        var pendingId = 0;
        var pendingIn = {};
        var pendingOut = {};
        var received = {};

        function addMessageToGroup(id, msg, parts) {
            if (!(id in pendingIn)) {
                pendingIn[id] = {
                    count: undefined,
                    msgs: [],
                    seq_no: pendingId++
                };
            }
            var group = pendingIn[id];
            group.msgs.push(msg);
            pendingCount++;
            var max_msgs = getMaxKeptCount();
            if ((max_msgs > 0) && (pendingCount > max_msgs)) {
                clearPending();
                node.error(RED._("switch.errors.too-many"), msg);
            }
            if (parts.hasOwnProperty("count")) {
                group.count = parts.count;
            }
            return group;
        }

        function drainMessageGroup(msgs, count, done) {
            var msg = msgs.shift();
            msg.parts.count = count;
            processMessage(msg, false, err => {
                if (err) {
                    done(err);
                } else {
                    if (msgs.length === 0) {
                        done()
                    } else {
                        drainMessageGroup(msgs, count, done);
                    }
                }
            })
        }
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        function addMessageToPending(msg, done) {
            var parts = msg.parts;
            // We've already checked the msg.parts has the require bits
            var group = addMessageToGroup(parts.id, msg, parts);
            var msgs = group.msgs;
            var count = group.count;
            var msgsCount = msgs.length;
            if (count === msgsCount) {
                // We have a complete group - send the individual parts
                drainMessageGroup(msgs, count, err => {
                    pendingCount -= msgsCount;
                    delete pendingIn[parts.id];
                    done();
                })
                return;
            }
            done();
        }

        function sendGroup(onwards, port_count) {
            var counts = new Array(port_count).fill(0);
            for (var i = 0; i < onwards.length; i++) {
                var onward = onwards[i];
                for (var j = 0; j < port_count; j++) {
                    counts[j] += (onward[j] !== null) ? 1 : 0
                }
            }
            var ids = new Array(port_count);
            for (var j = 0; j < port_count; j++) {
                ids[j] = RED.util.generateId();
            }
            var ports = new Array(port_count);
            var indexes = new Array(port_count).fill(0);
            for (var i = 0; i < onwards.length; i++) {
                var onward = onwards[i];
                for (var j = 0; j < port_count; j++) {
                    var msg = onward[j];
                    if (msg) {
                        var new_msg = RED.util.cloneMessage(msg);
                        var parts = new_msg.parts;
                        parts.id = ids[j];
                        parts.index = indexes[j];
                        parts.count = counts[j];
                        ports[j] = new_msg;
                        indexes[j]++;
                    } else {
                        ports[j] = null;
                    }
                }
                node.send(ports);

            }
        }

        function sendGroupMessages(onward, msg) {

            var parts = msg.parts;
            var gid = parts.id;
            received[gid] = ((gid in received) ? received[gid] : 0) + 1;
            var send_ok = (received[gid] === parts.count);

            if (!(gid in pendingOut)) {
                pendingOut[gid] = {
                    onwards: []
                };
            }

            var group = pendingOut[gid];
            var onwards = group.onwards;
            onwards.push(onward);
            pendingCount++;
            if (send_ok) {
                sendGroup(onwards, onward.length, msg);
                pendingCount -= onward.length;
                delete pendingOut[gid];
                delete received[gid];
            }
            var max_msgs = getMaxKeptCount();
            if ((max_msgs > 0) && (pendingCount > max_msgs)) {
                clearPending();
                node.error(RED._("switch.errors.too-many"), msg);
            }
        }
        //Data processing
        function processMessage(msg, checkParts, done) {
            var hasParts = msg.hasOwnProperty("parts") &&
                msg.parts.hasOwnProperty("id") &&
                msg.parts.hasOwnProperty("index");
            //Need to count the number of characters
            if (needsCount && checkParts && hasParts) {
                addMessageToPending(msg, done);
                //No need to count the number of characters
            } else {
                //get attribute msg
                getProperty(node, msg, (err, property) => {
                    if (err) {
                        node.warn(err);
                        done();
                    } else {
                        //Apply rules//////////////////////////////////////////////////////
                        applyRules(node, msg, property, undefined, (err, onward) => {
                            if (err) {
                                node.error(err, msg);
                            } else {
                                //need to compare characters
                                if (!repair || !hasParts) {
                                    node.send(onward);
                                } else {
                                    //output without comparing characters
                                    sendGroupMessages(onward, msg);

                                }
                            }
                            done();
                        });
                    }
                });
            }
        }

        function clearPending() {
            pendingCount = 0;
            pendingId = 0;
            pendingIn = {};
            pendingOut = {};
            received = {};
        }

        var pendingMessages = [];
        var handlingMessage = false;
        //process message queue
        var processMessageQueue = function(msg) {
            if (msg) {
                //port output
                // A new message has arrived - add it to the message queue
                pendingMessages.push(msg);
                if (handlingMessage) {
                    // The node is currently processing a message, so do nothing
                    // more with this message
                    return;
                }
            }
            if (pendingMessages.length === 0) {
                // There are no more messages to process, clear the active flag
                // and return
                handlingMessage = false;
                return;
            }

            // There are more messages to process. Get the next message and
            // start processing it. Recurse back in to check for any more
            var nextMsg = pendingMessages.shift();
            handlingMessage = true;
            // Callback
            processMessage(nextMsg, true, err => {
                if (err) {
                    node.error(err, nextMsg);
                }
                processMessageQueue()
            });
        };

        // if (node.wait_check)
        //     node.out = "time";
        //////////////////////////////////////////////////////////////////////////////////////////
        this.on("input", function(msg, nodeSend, nodeDone) {
            send_ok_flag = false;
            if (node.wait_check) {
                var sw_timer = this.interval_sw = setInterval(function() {
                    // node.emit("input", {});
                    var data_text = [];
                    //Inquire
                    for (var m = 0; m < Monitoring_data.length; m++) {
                        //Find the starting address in the global
                        if (Monitoring_data[m].address == node.addr_rd && Monitoring_data[m].server == node.server && Monitoring_data[m].port == node.port) {
                            for (var n = 0; n < node.num_rd; n++) {
                                var options = {};
                                options.id = node.addr_rd + n;
                                if (Monitoring_data[m + n].status == 1) {
                                    // Reset global flags, zero status   
                                    options.val = Monitoring_data[m + n].value;
                                } else {
                                    options.val = Monitoring_data[m + n].value = -1;
                                }
                                data_text.push(options);
                            }
                        }
                    }
                    //monitoring data
                    msg.arr_length = data_text.length;
                    msg.payload = data_text;
                    // console.log(data_text);
                    // console.log(Monitoring_data);
                    // console.log(Monitoring_data);
                    ////////////////////////////////////////////////////////////////////////////////
                    node.status({ fill: "green", shape: "dot", text: "Waiting" });
                    processMessageQueue(msg);
                    if (send_ok_flag) {
                        clearInterval(sw_timer);
                        console.log("stop");
                        node.status({});
                    }

                }, node.interval_sw_time);
            }

            /////////////////////////////////////////////////////////////////////////////////////////tcp
            // var i = 0;
            //convert input to string
            // if ((!Buffer.isBuffer(msg.payload)) && (typeof msg.payload !== "string")) {
            //     msg.payload = msg.payload.toString();
            // }
            //read and write mode
            if (node.model == "write") {
                // msg.payload = "WRS DM" + node.addr_wr + ".U" + " " + 1 + " " + node.num_wr + '\r\n';
                msg.payload = "WRITE " + node.addr_wr + " " + 1 + " " + node.num_wr + '\r\n';
            } else {
                // msg.payload = "RDS DM" + node.addr_rd + ".U" + " " + node.num_rd + '\r\n';
                msg.payload = "READ " + node.addr_rd + " " + node.num_rd + '\r\n';
            }

            var host = node.server || msg.host;
            var port = node.port || msg.port;

            // Store client information independently
            // the clients object will have:
            // clients[id].client, clients[id].msg, clients[id].timeout
            var connection_id = host + ":" + port;
            if (connection_id !== node.last_id) {
                node.status({});
                node.last_id = connection_id;
            }
            clients[connection_id] = clients[connection_id] || {
                msgQueue: new Denque(),
                connected: false,
                connecting: false
            };
            enqueue(clients[connection_id].msgQueue, { msg: msg, nodeSend: nodeSend, nodeDone: nodeDone });
            clients[connection_id].lastMsg = msg;

            if (!clients[connection_id].connecting && !clients[connection_id].connected) {
                var buf;
                if (this.out == "count") {
                    if (this.splitc === 0) { buf = Buffer.alloc(1); } else { buf = Buffer.alloc(this.splitc); }
                } else { buf = Buffer.alloc(65536); } // set it to 64k... hopefully big enough for most TCP packets.... but only hopefully

                clients[connection_id].client = net.Socket();
                if (socketTimeout !== null) { clients[connection_id].client.setTimeout(socketTimeout); }

                if (host && port) {
                    clients[connection_id].connecting = true;
                    clients[connection_id].client.connect(port, host, function() {
                        //node.log(RED._("tcpin.errors.client-connected"));
                        node.status({ fill: "green", shape: "dot", text: "common.status.connected" });
                        if (clients[connection_id] && clients[connection_id].client) {
                            clients[connection_id].connected = true;
                            clients[connection_id].connecting = false;
                            let event;
                            while (event = dequeue(clients[connection_id].msgQueue)) {
                                clients[connection_id].client.write(event.msg.payload);
                                event.nodeDone();
                            }
                            if (node.out === "time" && node.splitc < 0) {
                                clients[connection_id].connected = clients[connection_id].connecting = false;
                                clients[connection_id].client.end();
                                delete clients[connection_id];
                                node.status({});
                            }
                        }
                    });
                } else {
                    node.warn(RED._("tcpin.errors.no-host"));
                }
                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                //Timed trigger//to be processed

                ///////////////////////////////////////////////////////////
                clients[connection_id].client.on('data', function(data) {
                    if (node.out === "sit") { // if we are staying connected just send the buffer
                        if (clients[connection_id]) {
                            const msg = clients[connection_id].lastMsg || {};
                            data = data.toString().replace("\r\n", '').split(" ");
                            msg.payload = data;
                            ////////////////////////////////////////////////////////////////////////////////
                            // nodeSend(msg);
                            processMessageQueue(msg);
                            nodeSend(RED.util.cloneMessage(msg)); //sit
                        }

                    } else {
                        for (var j = 0; j < data.length; j++) {
                            if (node.out === "time") {
                                if (clients[connection_id]) {
                                    // do the timer thing
                                    if (clients[connection_id].timeout) {
                                        i += 1;
                                        buf[i] = data[j];
                                    } else {
                                        clients[connection_id].timeout = setTimeout(function() {
                                            if (clients[connection_id]) {
                                                clients[connection_id].timeout = null;
                                                const msg = clients[connection_id].lastMsg || {};
                                                msg.payload = Buffer.alloc(i + 1);
                                                buf.copy(msg.payload, 0, 0, i + 1);
                                                msg.payload = (msg.payload).toString();
                                                // nodeSend(msg); //time
                                                processMessageQueue(msg);
                                                console.log("write");
                                                if (clients[connection_id].client) {
                                                    node.status({});
                                                    clients[connection_id].client.destroy();
                                                    delete clients[connection_id];
                                                }
                                            }
                                        }, node.splitc);
                                        i = 0;
                                        buf[0] = data[j];
                                    }
                                }
                            }
                            // count bytes into a buffer...
                            else if (node.out == "count") {
                                buf[i] = data[j];
                                i += 1;
                                if (i >= node.splitc) {
                                    if (clients[connection_id]) {
                                        const msg = clients[connection_id].lastMsg || {};
                                        msg.payload = Buffer.alloc(i);
                                        buf.copy(msg.payload, 0, 0, i);
                                        nodeSend(msg); //count
                                        console.log("here3");
                                        if (clients[connection_id].client) {
                                            node.status({});
                                            clients[connection_id].client.destroy();
                                            delete clients[connection_id];
                                        }
                                        i = 0;
                                    }
                                }
                            }
                            // look for a char
                            else {
                                buf[i] = data[j];
                                i += 1;
                                if (data[j] == node.splitc) {
                                    if (clients[connection_id]) {
                                        const msg = clients[connection_id].lastMsg || {};
                                        msg.payload = Buffer.alloc(i);
                                        buf.copy(msg.payload, 0, 0, i);
                                        nodeSend(msg); //char
                                        console.log("here4");
                                        if (clients[connection_id].client) {
                                            node.status({});
                                            clients[connection_id].client.destroy();
                                            delete clients[connection_id];
                                        }
                                        i = 0;
                                    }
                                }
                            }
                        }
                    }
                });

                clients[connection_id].client.on('end', function() {
                    //console.log("END");
                    node.status({ fill: "grey", shape: "ring", text: "common.status.disconnected" });
                    if (clients[connection_id] && clients[connection_id].client) {
                        clients[connection_id].connected = clients[connection_id].connecting = false;
                        clients[connection_id].client = null;
                    }
                });

                clients[connection_id].client.on('close', function() {
                    //console.log("CLOSE");
                    if (clients[connection_id]) {
                        clients[connection_id].connected = clients[connection_id].connecting = false;
                    }

                    var anyConnected = false;

                    for (var client in clients) {
                        if (clients[client].connected) {
                            anyConnected = true;
                            break;
                        }
                    }
                    if (node.doneClose && !anyConnected) {
                        clients = {};
                        node.doneClose();
                    }
                });

                clients[connection_id].client.on('error', function() {
                    //console.log("ERROR");
                    node.status({ fill: "red", shape: "ring", text: "common.status.error" });
                    node.error(RED._("tcpin.errors.connect-fail") + " " + connection_id, msg);
                    if (clients[connection_id] && clients[connection_id].client) {
                        clients[connection_id].client.destroy();
                        delete clients[connection_id];
                    }
                });

                clients[connection_id].client.on('timeout', function() {
                    //console.log("TIMEOUT");
                    if (clients[connection_id]) {
                        clients[connection_id].connected = clients[connection_id].connecting = false;
                        node.status({ fill: "grey", shape: "dot", text: "tcpin.errors.connect-timeout" });
                        //node.warn(RED._("tcpin.errors.connect-timeout"));
                        if (clients[connection_id].client) {
                            clients[connection_id].connecting = true;
                            clients[connection_id].client.connect(port, host, function() {
                                clients[connection_id].connected = true;
                                clients[connection_id].connecting = false;
                                node.status({ fill: "green", shape: "dot", text: "common.status.connected" });
                            });
                        }
                    }
                });

            } else if (!clients[connection_id].connecting && clients[connection_id].connected) {
                if (clients[connection_id] && clients[connection_id].client) {
                    let event = dequeue(clients[connection_id].msgQueue)
                    clients[connection_id].client.write(event.msg.payload);
                    event.nodeDone();
                }
            }

            //////////////////////////////tcp
        });
        ///////////////////////////tcp
        this.on("close", function(done) {
            node.doneClose = done;
            for (var cl in clients) {
                if (clients[cl].hasOwnProperty("client")) {
                    clients[cl].client.destroy();
                }
            }
            node.status({});

            // this is probably not necessary and may be removed
            var anyConnected = false;
            for (var c in clients) {
                if (clients[c].connected) {
                    anyConnected = true;
                    break;
                }
            }
            if (!anyConnected) { clients = {}; }
            //clear timer
            if (node.onceTimeout) {
                clearTimeout(node.onceTimeout);
            }
            if (node.interval_sw != null) {
                clearInterval(node.interval_sw);
            }
            done();
            //////////////////////////tcp
            clearPending();
        });

    }
    RED.nodes.registerType("Keyence", SwitchNode);
    //////////////////////////////////////////////////////////////////////////
    //                                                                      //
    //                             TcpGet                                   //   
    //                                                                      //
    //////////////////////////////////////////////////////////////////////////
    function TcpGet(n) {
        RED.nodes.createNode(this, n);
        this.server = n.server;
        this.port = Number(n.port);
        this.interval_tcp_time = n.interval_tcp_time
        this.out = n.out;
        this.out = "sit";
        this.splitc = n.splitc;
        this.model = n.model;
        this.addr_rd = Number(n.RD_Addr);
        this.num_rd = Number(n.RD_Num);
        //monitor status
        var Monitoring_status = [];
        var data_second = [];
        var Monitoring_flag = 1;
        if (this.out === "immed") {
            this.splitc = -1;
            this.out = "time";
        }
        if (this.out !== "char") { this.splitc = Number(this.splitc); } else {
            if (this.splitc[0] == '\\') {
                this.splitc = parseInt(this.splitc.replace("\\n", 0x0A).replace("\\r", 0x0D).replace("\\t", 0x09).replace("\\e", 0x1B).replace("\\f", 0x0C).replace("\\0", 0x00));
            } // jshint ignore:line
            if (typeof this.splitc == "string") {
                if (this.splitc.substr(0, 2) == "0x") {
                    this.splitc = parseInt(this.splitc);
                } else {
                    this.splitc = this.splitc.charCodeAt(0);
                }
            } // jshint ignore:line
        }

        var node = this;

        var clients = {};
        //timing trigger
        if (node.interval_tcp_time) {
            this.interval_tcp = setInterval(function() {
                node.emit("input", {});
            }, node.interval_tcp_time);
        }
        //Delay trigger
        // this.onceTimeout=setTimeout(function() {
        //     // node.emit("input", {});
        //     // node.send("input");
        //     // node.repeaterSetup();
        // }, 5000);
        this.on("input", function(msg, nodeSend, nodeDone) {
            // if ((!Buffer.isBuffer(msg.payload)) && (typeof msg.payload !== "string")) {
            //     msg.payload = msg.payload.toString();
            // }
            if (node.model == "write") {
                msg.payload = "WRS DM" + node.addr_wr + ".U" + " " + 1 + " " + node.num_wr + '\r\n';
                // msg.payload = "WRITE " + node.addr_wr + " " + node.num_wr + '\r\n';
            } else {
                // msg.payload = "RDS DM" + node.addr_rd + ".U" + " " + node.num_rd + '\r\n';
                msg.payload = "READ " + node.addr_rd + " " + node.num_rd + '\r\n';
            }

            var host = node.server || msg.host;
            var port = node.port || msg.port;

            // Store client information independently
            // the clients object will have:
            // clients[id].client, clients[id].msg, clients[id].timeout
            var connection_id = host + ":" + port;
            if (connection_id !== node.last_id) {
                node.status({});
                node.last_id = connection_id;
            }
            clients[connection_id] = clients[connection_id] || {
                msgQueue: new Denque(),
                connected: false,
                connecting: false
            };
            enqueue(clients[connection_id].msgQueue, { msg: msg, nodeSend: nodeSend, nodeDone: nodeDone });
            clients[connection_id].lastMsg = msg;

            if (!clients[connection_id].connecting && !clients[connection_id].connected) {
                var buf;
                if (this.out == "count") {
                    if (this.splitc === 0) { buf = Buffer.alloc(1); } else { buf = Buffer.alloc(this.splitc); }
                } else { buf = Buffer.alloc(65536); } // set it to 64k... hopefully big enough for most TCP packets.... but only hopefully

                clients[connection_id].client = net.Socket();
                if (socketTimeout !== null) { clients[connection_id].client.setTimeout(socketTimeout); }

                if (host && port) {
                    clients[connection_id].connecting = true;
                    clients[connection_id].client.connect(port, host, function() {
                        //node.log(RED._("tcpin.errors.client-connected"));
                        node.status({ fill: "green", shape: "dot", text: "common.status.connected" });
                        if (clients[connection_id] && clients[connection_id].client) {
                            clients[connection_id].connected = true;
                            clients[connection_id].connecting = false;
                            let event;
                            while (event = dequeue(clients[connection_id].msgQueue)) {
                                clients[connection_id].client.write(event.msg.payload);
                                event.nodeDone();
                            }
                            if (node.out === "time" && node.splitc < 0) {
                                clients[connection_id].connected = clients[connection_id].connecting = false;
                                clients[connection_id].client.end();
                                delete clients[connection_id];
                                node.status({});
                            }
                        }
                    });
                } else {
                    node.warn(RED._("tcpin.errors.no-host"));
                }

                clients[connection_id].client.on('data', function(data) {
                    if (node.out === "sit") { // if we are staying connected just send the buffer
                        if (clients[connection_id]) {
                            const msg = clients[connection_id].lastMsg || {};
                            data = data.toString().replace("\r\n", '').split(" ");
                            //////////////////////////////////////////////////////////////////////////////////////output processing
                            // The first step: record the status, flip set to 1
                            for (var i = 0; i < data.length; i++) {
                                if (data[i] === data_second[i]) {
                                    Monitoring_flag = 0;
                                } else {
                                    if (Monitoring_flag === 0) Monitoring_status[i] = 1;
                                }
                            }
                            // save history
                            data_second = data;
                            //Step 2: Read the data and store it in the table
                            Monitoring_data = [];
                            var s = 0;
                            for (var key of data) {
                                var param = {}
                                param.server = host;
                                param.port = port;
                                param.address = node.addr_rd++;
                                param.value = Number(key);
                                param.status = Monitoring_status[s++]
                                Monitoring_data.push(param);
                            }
                            node.addr_rd = n.RD_Addr;
                            //Load global data for the first time
                            if (Monitoring_status.length == 0) {
                                for (var i = 0; i < Monitoring_data.length; i++) {
                                    if (Monitoring_data[i].value) {
                                        Monitoring_status[i] = 1;
                                    }
                                }
                            }

                            // msg.payload = data;
                            // console.log(Monitoring_status);
                            // console.log(Monitoring_data);
                            ////////////////////////////////////////////////////////////////////////////////////////
                            nodeSend(RED.util.cloneMessage(msg));
                        }
                    }
                    // else if (node.splitc === 0) {
                    //     clients[connection_id].msg.payload = data;
                    //     node.send(clients[connection_id].msg);
                    // }
                    else {
                        for (var j = 0; j < data.length; j++) {
                            if (node.out === "time") {
                                if (clients[connection_id]) {
                                    // do the timer thing
                                    if (clients[connection_id].timeout) {
                                        i += 1;
                                        buf[i] = data[j];
                                    } else {
                                        clients[connection_id].timeout = setTimeout(function() {
                                            if (clients[connection_id]) {
                                                clients[connection_id].timeout = null;
                                                const msg = clients[connection_id].lastMsg || {};
                                                msg.payload = Buffer.alloc(i + 1);
                                                buf.copy(msg.payload, 0, 0, i + 1);
                                                nodeSend(msg);
                                                if (clients[connection_id].client) {
                                                    node.status({});
                                                    clients[connection_id].client.destroy();
                                                    delete clients[connection_id];
                                                }
                                            }
                                        }, node.splitc);
                                        i = 0;
                                        buf[0] = data[j];
                                    }
                                }
                            }
                            // count bytes into a buffer...
                            else if (node.out == "count") {
                                buf[i] = data[j];
                                i += 1;
                                if (i >= node.splitc) {
                                    if (clients[connection_id]) {
                                        const msg = clients[connection_id].lastMsg || {};
                                        msg.payload = Buffer.alloc(i);
                                        buf.copy(msg.payload, 0, 0, i);
                                        nodeSend(msg);
                                        if (clients[connection_id].client) {
                                            node.status({});
                                            clients[connection_id].client.destroy();
                                            delete clients[connection_id];
                                        }
                                        i = 0;
                                    }
                                }
                            }
                            // look for a char
                            else {
                                buf[i] = data[j];
                                i += 1;
                                if (data[j] == node.splitc) {
                                    if (clients[connection_id]) {
                                        const msg = clients[connection_id].lastMsg || {};
                                        msg.payload = Buffer.alloc(i);
                                        buf.copy(msg.payload, 0, 0, i);
                                        nodeSend(msg);
                                        if (clients[connection_id].client) {
                                            node.status({});
                                            clients[connection_id].client.destroy();
                                            delete clients[connection_id];
                                        }
                                        i = 0;
                                    }
                                }
                            }
                        }
                    }
                });

                clients[connection_id].client.on('end', function() {
                    //console.log("END");
                    node.status({ fill: "grey", shape: "ring", text: "common.status.disconnected" });
                    if (clients[connection_id] && clients[connection_id].client) {
                        clients[connection_id].connected = clients[connection_id].connecting = false;
                        clients[connection_id].client = null;
                    }
                });

                clients[connection_id].client.on('close', function() {
                    //console.log("CLOSE");
                    if (clients[connection_id]) {
                        clients[connection_id].connected = clients[connection_id].connecting = false;
                    }

                    var anyConnected = false;

                    for (var client in clients) {
                        if (clients[client].connected) {
                            anyConnected = true;
                            break;
                        }
                    }
                    if (node.doneClose && !anyConnected) {
                        clients = {};
                        node.doneClose();
                    }
                });

                clients[connection_id].client.on('error', function() {
                    //console.log("ERROR");
                    node.status({ fill: "red", shape: "ring", text: "common.status.error" });
                    node.error(RED._("tcpin.errors.connect-fail") + " " + connection_id, msg);
                    if (clients[connection_id] && clients[connection_id].client) {
                        clients[connection_id].client.destroy();
                        delete clients[connection_id];
                    }
                });

                clients[connection_id].client.on('timeout', function() {
                    //console.log("TIMEOUT");
                    if (clients[connection_id]) {
                        clients[connection_id].connected = clients[connection_id].connecting = false;
                        node.status({ fill: "grey", shape: "dot", text: "tcpin.errors.connect-timeout" });
                        //node.warn(RED._("tcpin.errors.connect-timeout"));
                        if (clients[connection_id].client) {
                            clients[connection_id].connecting = true;
                            clients[connection_id].client.connect(port, host, function() {
                                clients[connection_id].connected = true;
                                clients[connection_id].connecting = false;
                                node.status({ fill: "green", shape: "dot", text: "common.status.connected" });
                            });
                        }
                    }
                });
            } else if (!clients[connection_id].connecting && clients[connection_id].connected) {
                if (clients[connection_id] && clients[connection_id].client) {
                    let event = dequeue(clients[connection_id].msgQueue)
                    clients[connection_id].client.write(event.msg.payload);
                    event.nodeDone();
                }
            }
        });

        this.on("close", function(done) {
            node.doneClose = done;
            for (var cl in clients) {
                if (clients[cl].hasOwnProperty("client")) {
                    clients[cl].client.destroy();
                }
            }
            node.status({});

            // this is probably not necessary and may be removed
            var anyConnected = false;
            for (var c in clients) {
                if (clients[c].connected) {
                    anyConnected = true;
                    break;
                }
            }
            if (!anyConnected) { clients = {}; }
            //clear timer
            if (node.onceTimeout) {
                clearTimeout(this.onceTimeout);
            }
            if (node.interval_tcp != null) {
                clearInterval(node.interval_tcp);
            }
            done();
        });

    }
    RED.nodes.registerType("Read address", TcpGet);
}
