import hostapdswitch = require("hostapd_switch");
import * as Promise from "bluebird";
import * as fs from "fs";
import * as _ from "lodash";
import testinternet = require("promise-test-connection");
import merge = require("json-add");
let netw = require("netw");
let network = require('network');
let LMC = require('linux-mobile-connection');
let mobileconnect = require('linux-mobile-connection');
let verb = require('verbo');


function recovery_mode(config: { hostapd: {} }, dev: string) {

    let confhapds = {
        interface: dev,
        hostapd: config.hostapd
    }

    let apswitch = new hostapdswitch(confhapds);

    return new Promise(function(resolve, reject) {
        apswitch.host().then(function(answer) {
            verb(answer, 'warn', 'linetwork recovery mode')
            resolve(answer)
        }).catch(function(err) {
            verb(err, 'error', 'linetwork recovery mode failed')
            reject(err)
        })
    })
}

interface ClassOpt {
    recovery?: boolean;
    port?: number;
    recovery_interface?: string;
}
interface IMobile {
provider:string;
options:{
}
}
interface ILiNetworkConf {
    recovery: boolean;
    port: number;
    recovery_interface: string;
    mobile?:IMobile
}

let config: ILiNetworkConf = {
    recovery: true,
    port: 4000, // in modalità regular setta la porta per il manager
    // wpa_supplicant_path:'/etc/wpa_supplicant/wpa_supplicant.conf',
    recovery_interface: 'auto'
}


export =class LiNetwork {
    config: ILiNetworkConf;
    constructor(public data?: ClassOpt) {
        merge(config, data)
        this.config = config
    }

    wifi_switch = function(mode: string, dev?: string) {
        console.log(mode, dev);
        if (dev || this.config.recovery_interface != 'auto') {
            if (dev) {
                var apswitch = new hostapdswitch(
                    {
                        interface: dev,
                        hostapd: this.hostapd
                    }
                );
            } else {
                var apswitch = new hostapdswitch(
                    {
                        interface: this.config.recovery_interface,
                        hostapd: this.hostapd
                    }
                );
            }
            console.log('dev mode')
            return new Promise(function(resolve, reject) {
                switch (mode) {
                    case 'ap':
                        apswitch.ap().then(function(answer) {
                            resolve(answer)
                        }).catch(function(err) {
                            reject(err)
                        })
                        break;

                    case 'host':
                        apswitch.host().then(function(answer) {
                            resolve(answer)
                        }).catch(function(err) {
                            reject(err)
                        })
                        break;

                    case 'client':
                        apswitch.client().then(function(answer) {
                            resolve(answer)
                        }).catch(function(err) {
                            reject(err)
                        })
                        break;

                };

            });

        } else {
            console.log('auto mode')
            var config = this.config;
            return new Promise(function(resolve, reject) {
                netw().then(function(data) {
                    console.log(data)
                    _.map(data.networks, function(device: { type: string, interface: string }) {
                        if (device.type == 'wifi') {
                            dev = device.interface
                        }
                    })
                    if (dev) {
                        var apswitch = new hostapdswitch(
                            {
                                interface: dev,
                                hostapd: config.hostapd
                            }
                        );

                        console.log(apswitch)

                        switch (mode) {
                            case 'ap':
                                apswitch.ap().then(function(answer) {
                                    resolve(answer)
                                }).catch(function(err) {
                                    reject(err)
                                })
                                break;

                            case 'host':
                                apswitch.host().then(function(answer) {
                                    resolve(answer)
                                }).catch(function(err) {
                                    reject(err)
                                })
                                break;

                            case 'client':
                                apswitch.client().then(function(answer) {
                                    resolve(answer)
                                }).catch(function(err) {
                                    reject(err)
                                })
                                break;
                        }

                    } else {
                        reject({ error: 'no dev' })
                    }
                }).catch(function(err) {
                    reject(err)
                })
            })
        }
    };
    
    mproviders = function() {
        return JSON.parse(fs.readFileSync(__dirname + '/node_modules/linux-mobile-connection/node_modules/wvdialjs/providers.json', "utf-8"))
    };
    
    init = function() {
        let config = this.config;
        return new Promise(function(resolve, reject) {
            verb(config, 'debug', 'Tryng to connect')
            network.get_public_ip(function(err, ip) {
                if (err) {
                    let wifi_exist: any = false;
                    netw().then(function(net) {
                        console.log(net.networks)
                        _.map(net.networks, function(device: { type: string, interface: string }) {

                            if (device.type == 'wifi' && (!config.recovery_interface || config.recovery_interface == 'auto' || config.recovery_interface == device.interface)) {
                                wifi_exist = device.interface
                            }
                        })
                        console.log(wifi_exist)

                        if (wifi_exist) {
                            var confhapds = {
                                interface: wifi_exist,
                                hostapd: config.hostapd
                            }

                            verb(wifi_exist, 'info', 'Wlan interface founded');
                            var apswitch = new hostapdswitch(confhapds);
                            apswitch.client().then(function(answer) {
                                resolve(answer)
                            }).catch(function(err) {
                                if (config.mobile) {
                                    LMC(config.mobile.provider, config.mobile.options).then(function(answer) {
                                        resolve(answer)
                                    }).catch(function() {
                                        if (config.recovery) {
                                            recovery_mode(config, wifi_exist).then(function(answer) {
                                                resolve(answer)
                                            }).catch(function(err) {
                                                verb(err, 'error', 'J5 recovery mode start')
                                                reject(err)
                                            })
                                        } else {
                                            reject('no wlan host available')
                                        }
                                    })
                                } else if (config.recovery) {
                                    recovery_mode(config, wifi_exist).then(function(answer) {
                                        resolve(answer)
                                    }).catch(function(err) {
                                        verb(err, 'error', 'J5 recovery mode start')
                                        reject(err)
                                    })
                                }
                            })
                        } else {
                            verb('no wifi', 'warn', 'networker')

                            if (config.mobile) {
                                LMC(config.mobile.provider, config.mobile.options).then(function(answer) {
                                    resolve(answer)
                                }).catch(function(err) {
                                    verb(err, 'error', 'J5 linuxmobile')
                                    reject(err)
                                })
                            }
                        }

                    }).catch(function(err) {
                        verb(err, 'error', 'netw linuxmobile')
                        reject(err)
                    })
                } else {
                    resolve({ connected: true })
                }

            })
        })
    };

    recovery = function(dev) {
        return recovery_mode(this.config, dev)
    };

};






