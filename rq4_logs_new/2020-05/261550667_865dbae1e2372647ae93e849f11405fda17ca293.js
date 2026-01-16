// ==UserScript==
// @name CoTG Lionnel0 local
// @namespace https://bitbucket.org/
// @version 0.3.0
// @description Lionnel0's CoTG Tools
// @author Lionnel0
// @match https://w*.crownofthegods.com/
// @include https://w*.crownofthegods.com/
// @grant none
// @updateURL    https://bitbucket.org/lionnel0/cotg/raw/master/lionnel.js
// @doc    https://bitbucket.org/lionnel0/cotg_public/wiki/Home
// ==/UserScript==
(function Lionnel0Script() {
    const LIO_DIV = "lioDiv";
    const LIO_DIV_DIV_SELECTOR = "#" + LIO_DIV + ">div";
    const BUTTON_CITY_INFO_SELECTOR = "#" + LIO_DIV + ">div:nth-child(2)>button";
    const SELECT_CITY_CLEAR_SELECTOR = "#" + LIO_DIV + ">div:nth-child(2)>selector:nth-child(1)";
    const SELECT_CITY_MOVE_ID = "lioCityMove";
    const SELECT_CITY_ADD_ID = "lioCityAdd";
    const CSS_FIRST_BUTTON = "margin-left:8px;margin-top:10px;width:74px;font-size:10px !important;";
    const CSS_NEXT_BUTTON = "margin-left:4px;margin-top:10px;width:22%;font-size:10px !important;";
    const CLASS_AND_CSS_FIRST_BUTTON = 'style="' + CSS_FIRST_BUTTON + '" class="regButton greenb"';
    const CLASS_AND_CSS_NEXT_BUTTON = 'style="' + CSS_NEXT_BUTTON + '" class="regButton greenb"';

    var LIO = {
        _version: "0.3.0"
    };

    // Facility to have building informations
    LIO.BUILDINGS = new function () {
        var that = LIO;

        // Inner data to characterise buildings
        that._DETAILS = {
            // Resources getters
            forester: {
                id: [448],
                buildable: true,
                schedule: "2",
                shortcut: "f",
                movable: true
            },
            quarry: {
                id: [461],
                buildable: true,
                schedule: "3",
                shortcut: "s",
                movable: true
            },
            farmhouse: {
                id: [447],
                buildable: true,
                schedule: "1",
                shortcut: "a",
                movable: true
            },
            mine: {
                id: [465],
                buildable: true,
                schedule: "4",
                shortcut: "i",
                movable: true
            },

            // Resources helpers
            sawmill: {
                id: [460],
                buildable: true,
                schedule: "L",
                shortcut: "l",
                movable: true
            },
            stonemason: {
                id: [462],
                buildable: true,
                schedule: "A",
                shortcut: "h",
                movable: true
            },
            windmill: {
                id: [463],
                buildable: true,
                schedule: "M",
                shortcut: "g",
                movable: true
            },
            smelter: {
                id: [477],
                buildable: true,
                schedule: "D",
                shortcut: "z",
                movable: true
            },

            // Others
            storehouse: {
                id: [464],
                buildable: true,
                schedule: "S",
                shortcut: "r",
                movable: true
            },
            cottage: {
                id: [446],
                buildable: true,
                schedule: "C",
                shortcut: "c",
                movable: true
            },
            hideaway: {
                id: [479],
                buildable: true,
                schedule: "H",
                shortcut: "q",
                movable: true
            },
            townhouse: {
                id: [481],
                buildable: true,
                schedule: "U",
                shortcut: "v",
                movable: true
            }, // i.e. villa: useless. Raid !

            // Not builded
            townhall: {
                id: [455],
                buildable: false,
                schedule: "T"
            },
            castle: {
                id: [467],
                buildable: false,
                schedule: "X",
                // shortcut: "x", too dangerous !!!
                movable: true
            },
            temple: {
                id: [1000],
                buildable: false
            }, // TODO update id

            // Trade
            port: {
                id: [488, 489, 490],
                buildable: true,
                schedule: "R",
                shortcut: "o",
                movable: true
            },
            marketplace: {
                id: [449],
                buildable: true,
                schedule: "P",
                shortcut: "m",
                movable: true
            }, // i.e. forum

            // Military
            cityguardhouse: {
                id: [504],
                buildable: true,
                schedule: "K",
                shortcut: "u",
                movable: true
            }, // quite useless
            barracks: {
                id: [445],
                buildable: true,
                schedule: "B",
                shortcut: "b",
                movable: true
            },
            trainingground: {
                id: [483],
                buildable: true,
                schedule: "G",
                shortcut: "t",
                movable: true
            },
            stable: {
                id: [466],
                buildable: true,
                schedule: "E",
                shortcut: "e",
                movable: true
            },
            mage_tower: {
                id: [500],
                buildable: true,
                schedule: "J",
                shortcut: "w",
                movable: true
            },
            academy: {
                id: [482],
                buildable: true,
                schedule: "Z",
                shortcut: "y",
                movable: true
            },
            blacksmith: {
                id: [502],
                buildable: true,
                schedule: "Y",
                shortcut: "k",
                movable: true
            },
            shipyard: {
                id: [491, 496, 498],
                buildable: true,
                schedule: "V",
                shortcut: "p",
                movable: true
            },

            // Resources
            forest: {
                id: [454],
                buildable: false,
                schedule: "."
            },
            stone: {
                id: [451],
                buildable: false,
                schedule: ":"
            },
            iron: {
                id: [452],
                buildable: false,
                schedule: ","
            },
            lake: {
                id: [453],
                buildable: false,
                schedule: ";"
            }

            // TODO: add wall & towers
        };

        // Same as _DETAILS but in a list.
        that._LIST = [];

        // Hash buildings from city planner letter.
        that._FromMapLetter = {};

        Object.keys(that._DETAILS).forEach(function (key) {
            that._DETAILS[key].name = key;
            that._LIST.push(that._DETAILS[key]);
            if (that._DETAILS[key].schedule) {
                that._FromMapLetter[that._DETAILS[key].schedule] = that._DETAILS[key];
            }
        });

        // Is a builting type buildable. For information, due to dangerousness of building castles, the operation has to be manual.
        that.isBuildable = function (btype) {
            return that._LIST.filter(function (item) {
                return (item.id.indexOf(btype) >= 0) && (item.buildable);
            }).length > 0;
        };

        // Return building from a btype (if known)
        that.buildingFromId = function (btype) {
            return that._LIST.find(function (item) {
                return (item.id.indexOf(btype) >= 0);
            });
        };

        // Searching town hall.
        that.isTownHall = function (btype) {
            return that._DETAILS.townhall.id[0] === btype;
        };

        // Return true if has farm or lakes in plan => i.e. food city.
        that.needToClearAllResources = function (scheduleMap) {
            if (scheduleMap.length === 0) return false;
            return scheduleMap.some(function (building) {
                return (building === that._DETAILS.farmhouse) ||
                    //(building === that._DETAILS.windmill) ||
                    (building === that._DETAILS.lake);
            });
        };
        // Update curentBuilding status regarding schedule
        // error: true if wanted to build another resource than existing one.
        // toDestroy: true if it's curently a resource and want a building, or if it's curently a resource and needToClearAllResources
        // toMove: true if (not error, nor to destroy), and curent and schedule do not match, and movable.
        //      spare building are set to toMove at this step.
        // toAdd: true if (not error, nor to destroy, nor to move), and schedule can be built
        that.updateStatus = function (curentBuilding, scheduleBuilding, needToClearAllResources) {
            curentBuilding.error = false;
            curentBuilding.toDestroy = false;
            curentBuilding.toMove = false;
            curentBuilding.toAdd = false;
            if (scheduleBuilding) {
                if (scheduleBuilding.id.indexOf(curentBuilding.btype) >= 0) {
                    // Good building !
                } else if (scheduleBuilding === that._DETAILS.forest ||
                    scheduleBuilding === that._DETAILS.stone ||
                    scheduleBuilding === that._DETAILS.iron ||
                    scheduleBuilding === that._DETAILS.lake) {
                    // Resource planed but not in curent
                    curentBuilding.error = true;
                } else if (curentBuilding.btype === that._DETAILS.forest.id[0] ||
                    curentBuilding.btype === that._DETAILS.stone.id[0] ||
                    curentBuilding.btype === that._DETAILS.iron.id[0] ||
                    curentBuilding.btype === that._DETAILS.lake.id[0]) {
                    curentBuilding.toDestroy = !curentBuilding.demolishing; // Something else to build
                } else if (!curentBuilding.building) {
                    if (scheduleBuilding.buildable) {
                        curentBuilding.toAdd = true;
                        curentBuilding.buildingToAdd = scheduleBuilding;
                    }
                } else if (curentBuilding.building.movable) {
                    // Potentially to move
                    curentBuilding.toMove = !curentBuilding.demolishing;
                } else {
                    debugger;
                }
                return false;
            }
            if (curentBuilding) {
                // Curent but not planed
                if (!curentBuilding.building) {
                    return; // Unknown building type.
                } else if (curentBuilding.btype === that._DETAILS.forest.id[0] ||
                    curentBuilding.btype === that._DETAILS.stone.id[0] ||
                    curentBuilding.btype === that._DETAILS.iron.id[0] ||
                    curentBuilding.btype === that._DETAILS.lake.id[0]) {
                    curentBuilding.toDestroy = needToClearAllResources && (!curentBuilding.demolishing);
                } else if (curentBuilding.building.movable) {
                    // Potentially to move
                    curentBuilding.toMove = !curentBuilding.demolishing;
                } else if (!curentBuilding.building.buildable) {
                    // RAS
                } else {
                    debugger;
                }
                return false;
            }
            return false;
        };

        // Count the number of building and transforming to move in to destroy for spare buildings.
        that.checkMove = function (curentMap, scheduleMap) {
            // Counting in the map per type
            var nbOfBuildingScheduledPerType = scheduleMap.reduce(function (pv, cv) {
                if (cv) {
                    if (pv[cv.name]) {
                        pv[cv.name] = pv[cv.name] + 1;
                    } else {
                        pv[cv.name] = 1;
                    }
                }
                return pv;
            }, {});
            // Removing already ok buildings
            for (var buildingIndex in curentMap) {
                var curent = curentMap[buildingIndex];
                var schedule = scheduleMap[buildingIndex];

                if (curent && schedule && (curent.building === schedule)) {
                    if (nbOfBuildingScheduledPerType[schedule.name] > 0) {
                        nbOfBuildingScheduledPerType[schedule.name] = nbOfBuildingScheduledPerType[schedule.name] - 1;
                    }
                }
            }
            // Checking to move
            curentMap.forEach(function (curent) {
                if (curent.toMove) {
                    if ((!nbOfBuildingScheduledPerType[curent.building.name]) ||
                        (nbOfBuildingScheduledPerType[curent.building.name] === 0)) {
                        curent.toMove = false;
                        curent.toDestroy = !curent.demolishing;
                    } else {
                        nbOfBuildingScheduledPerType[curent.building.name] = nbOfBuildingScheduledPerType[curent.building.name] - 1;
                    }
                }
            });
        };

        // Return the building from a plan letter.
        that.buildingFromMapLetter = function (letter) {
            return that._FromMapLetter[letter];
        };

        return that;
    }();
    // Build manager constructor
    LIO.buildManager = new function () {
        var that = LIO;

        // Id to identify curent city (cid in CoTG)
        that.data = {
            id: null
        };

        // CoTG city data parser
        that.parseCurentCityData = function (cdata) {
            var updatedData = false;

            if (that.data.id !== cdata.cid) {
                updatedData = true;
                that.data = {
                    id: cdata.cid,
                    curentMap: [],
                    scheduleMap: [],
                    autoModeActivated: that.data.autoModeActivated
                };
                that.message = {
                    map: "No map"
                };
            }

            // Coordinates
            //that.data.x=Number(cdata.cid % 65536);
            //that.data.y=Number((cdata.cid-that.data.x)/65536);
            //that.data.cont=Number(Math.floor(that.data.x/100)+10*Math.floor(that.data.y/100));

            //that.data.th = cdata.th; // idleTroops
            //that.data.mo = cdata.mo; // Councilors configuration.

            // Current map
            if (cdata.bd) {
                updatedData = true;
                // Updating buildings count for city
                that.data.townhallLevel = 1;
                that.data.curentMap = [];

                // Filling curentMap
                for (var buildingIndex in cdata.bd) {
                    var btype = Number(cdata.bd[buildingIndex].bid);
                    that.data.curentMap.push({
                        btype: btype,
                        index: buildingIndex,
                        building: that.BUILDINGS.buildingFromId(btype)
                    });
                    if (that.BUILDINGS.isTownHall(btype)) {
                        that.data.townhallLevel = cdata.bd[buildingIndex].bl;
                    }
                }
            }

            if (cdata.bq) {
                updatedData = true;
                // Processing building queue
                cdata.bq.forEach(function (buildQueueItem) {
                    var index = buildQueueItem.bspot;
                    if (that.data.curentMap[index]) {
                        if (buildQueueItem.elvl === 0) {
                            that.data.curentMap[index].demolishing = true;
                        } else if (buildQueueItem.slvl === 0) {
                            // New building in queue
                            var btype = buildQueueItem.brep;
                            that.data.curentMap[index].btype = btype;
                            that.data.curentMap[index].building = that.BUILDINGS.buildingFromId(btype);
                        }
                    }
                });
            }

            // Schedule map.
            if (cdata.sts) {
                updatedData = true;
                that.data.scheduleMap = [];
                var stsFormat = cdata.sts.substring(0, 18);
                if (stsFormat == ("[ShareString.1.3]:") || stsFormat == "[ShareString.1.3];") {
                    that.message.map = "";
                    var stsData = cdata.sts.substring(18, 441).split(""); // to char array
                    for (var index = 0; index < stsData.length; index++) {
                        var letter = stsData[index];
                        if ("#-_".indexOf(letter) >= 0) { // RAS no building
                            that.data.scheduleMap.push(null);
                        } else {
                            var building = that.BUILDINGS.buildingFromMapLetter(letter);
                            if (!building) {
                                that.message.map = "Unknown type of building '" + letter + "'. Please report the bug.";
                                that.data.scheduleMap = [];
                                break;
                            }
                            that.data.scheduleMap.push(building);
                        }
                    }
                } else {
                    that.message.map = "Unknwon map format. Please report the evolution.";
                }
            }

            that.onCityDataUpdated(updatedData);
        }

        // Utils
        that.utils = {
            // DOM operation on object:
            // jquery selector,
            // remove some classes
            // add some classes
            // set a title
            classSwitch: function (buttonSelector, classToRemove, classToAdd, title) {
                buttonSelector.removeClass(classToRemove);
                buttonSelector.addClass(classToAdd);
                buttonSelector.attr("title", title);
            },
            // display a message in bottom rigth
            errorMsg: {
                display: function (message) {
                    this._id = this._id + 1;
                    var ident = this._idKey + this._id;
                    var errormsgs = '<tr ID = "' + ident + '"><td><div class = "errBR">' + message + '<div></td></tr>';
                    $("#errorBRpopup").append(errormsgs);
                    // Show
                    $("#" + ident).show();
                    $("#" + ident + " div").animate({
                        opacity: 1,
                        bottom: "+10px"
                    }, 'slow');
                    // Hide
                    setTimeout(function () {
                        $("#" + ident + " div").animate({
                            opacity: 0,
                            bottom: "-10px"
                        }, 'slow');
                        $("#" + ident).fadeOut("slow");
                    }, 5000);
                    setTimeout(function () {
                        $("#" + ident).remove();
                    }, 6000);
                },
                _idKey: "lioErrBR",
                _id: 0
            }
        };

        // Building added data (re computed ones) & update HMI
        that.previousViewData = {};
        that.onCityDataUpdated = function (updatedData) {
            if (updatedData) {
                that.addedData = {
                    needToClearResources: that.BUILDINGS.needToClearAllResources(that.data.scheduleMap),
                    pureNavy: false,
                    pureTrade: false
                };

                // Case of warship & stingers
                var partialMap = that.data.scheduleMap.filter(function (item) {
                    return item && item.buildable
                }).length <= 60;
                if (partialMap) {
                    if (that.data.scheduleMap.filter(function (item) {
                            return item === that.BUILDINGS._DETAILS.shipyard
                        }).length === 8) {
                        that.addedData.pureNavy = true;
                    }
                }
                if (that.data.scheduleMap.filter(function (item) {
                        return item === that.BUILDINGS._DETAILS.marketplace
                    }).length >= 20) {
                    that.addedData.pureTrade = true;
                }

                // Post processing data regarding scheduledMap
                if (that.data.scheduleMap.length > 0 && that.data.curentMap.length > 0) {
                    for (var buildingIndex in that.data.scheduleMap) {
                        if (that.data.scheduleMap[buildingIndex]) {
                            that.data.curentMap[buildingIndex].schedule = that.data.scheduleMap[buildingIndex];
                        }
                        that.BUILDINGS.updateStatus(
                            that.data.curentMap[buildingIndex],
                            that.data.scheduleMap[buildingIndex],
                            that.addedData.needToClearResources);
                    }

                    // 2nd check to move
                    that.BUILDINGS.checkMove(that.data.curentMap, that.data.scheduleMap);
                }
            }

            // Building new view data
            var mapSetOk = (that.data.scheduleMap.length !== 0 && that.data.curentMap.length !== 0);
            var infoMsg = that.message.map;
            var buildingsToDestroy = that.data.curentMap.filter(function (item) {
                return item.toDestroy;
            });
            var buildingsToMove = that.data.curentMap.filter(function (item) {
                return item.toMove;
            });
            var buildingsToAdd = that.data.curentMap.filter(function (item) {
                return item.toAdd;
            });
            var divButtons = mapSetOk ? [{ // Button info
                title: infoMsg,
                removeClass: "redb greenb",
                addClass: that.data.autoModeActivated ? "redb" : "greenb",
                text: that.data.autoModeActivated ? "Activated" : "Deactivated"
            }, { // Selector Clear
                title: buildingsToDestroy.length > 0 ? "" : "Nothing to destroy",
                removeClass: "disable greenb",
                addClass: buildingsToDestroy.length > 0 ? "greenb" : "disable",
                options: buildingsToDestroy.length === 0 ? [{
                    text: "Clean !"
                }] : [{
                    text: buildingsToDestroy.length + " Clear"
                }].concat(
                    buildingsToDestroy.reduce(function (pv, cv) {
                        return pv.concat([{
                            index: cv.index,
                            text: cv.building.name
                        }]);
                    }, []))
            }, { // Move selector
                title: buildingsToMove.length > 0 ? "" : "Nothing to move",
                removeClass: "disable greenb",
                addClass: buildingsToMove.length > 0 ? "greenb" : "disable",
                options: buildingsToMove.length === 0 ? [{
                    text: "In place !"
                }] : [{
                    text: buildingsToMove.length + " Move"
                }].concat(
                    buildingsToMove.reduce(function (pv, cv) {
                        return pv.concat([{
                            index: cv.index,
                            text: cv.building.name
                        }]);
                    }, []))
            }, { // Add selector
                title: buildingsToAdd.length > 0 ? "" : "Nothing to add",
                removeClass: "disable greenb",
                addClass: buildingsToAdd.length > 0 ? "greenb" : "disable",
                options: buildingsToAdd.length === 0 ? [{
                    text: "Complete !"
                }] : [{
                    text: buildingsToAdd.length + " Add"
                }].concat(
                    buildingsToAdd.reduce(function (pv, cv) {
                        return pv.concat([{
                            index: cv.index,
                            text: cv.buildingToAdd.name
                        }]);
                    }, []))
            }] : [];

            var currentViewData = {
                children: [{ // Div info
                    visible: !mapSetOk,
                    text: 'Set a map to activate functionnality',
                    title: infoMsg
                }, { // Div button bar
                    visible: mapSetOk,
                    children: divButtons
                }]
            };

            /*
            // Updating city buttons
            var errors = that.data.curentMap.filter(function (item) {
                return item.error;
            });
            if (errors.length > 0) {
                infoMsg = infoMsg + "\n" + errors.length + " errors";
                $(BUTTON_CITY_INFO_SELECTOR).attr("style", CSS_FIRST_BUTTON + "color:red;");
            } else {
                $(BUTTON_CITY_INFO_SELECTOR).attr("style", CSS_FIRST_BUTTON);
            }
            if (that.data.scheduleMap.length === 0 || that.data.curentMap.length === 0) {
                if (infoMsg.length === 0) {
                    infoMsg = "No current map. Switch city";
                }
            */

            if (JSON.stringify(that.previousViewData) !== JSON.stringify(currentViewData)) {
                // Updating HMI.
                that.previousViewData = currentViewData;
                var updateItem = function (selector, data) {
                    Object.keys(data).forEach(function (key) {
                        var classes = false;
                        switch (key) {
                            case "children":
                                {
                                    var childrenSelector = selector.children();
                                    data[key].forEach(function (childData, index) {
                                        updateItem(childrenSelector.eq(index), childData);
                                    });
                                }
                                break;
                            case "visible":
                                {
                                    if ((selector.css('display') !== 'none') !== data[key]) {
                                        selector.toggle();
                                    }
                                }
                                break;
                            case "text":
                                {
                                    selector.html(data[key]);
                                }
                                break;
                            case "title":
                                {
                                    selector.attr("title", data[key]);
                                }
                                break;
                            case "removeClass":
                            case "addClass":
                                {
                                    classes = true;
                                }
                                break;
                            case "options":
                                {
                                    selector.html(data[key].reduce(
                                        function (pv, cv) {
                                            var index = cv.index ? "index='" + cv.index + "'" : "";
                                            return pv + "<option " + index + ">" + cv.text + "</option>"
                                        }, ""));
                                }
                                break;
                            default:
                                console.error("Todo " + key);
                                debugger;
                        };
                        if (classes) {
                            selector.removeClass(data["removeClass"]);
                            selector.addClass(data["addClass"]);
                        }
                    })

                }
                updateItem($("#" + LIO_DIV), currentViewData);
            }
        };

        // What to do with current building
        that.processBuilding = function () {
            if (that.data.autoModeActivated) {
                var selectedBuilding = $("#city_map .hoverbuild").attr("data");
                if ((selectedBuilding) && that.data.curentMap[selectedBuilding]) {
                    var curentBuilding = that.data.curentMap[selectedBuilding];
                    var toAdd = null;
                    if (curentBuilding.toDestroy) {
                        that.utils.errorMsg.display("Deleting this resource or building");
                        $("#buildingDemolishButton").trigger({
                            type: "click",
                            originalEvent: "1"
                        });
                    } else if (curentBuilding.toMove) {
                        // Searching a free space.
                        var curentBuildingIndex = curentBuilding.index;
                        var found = false;
                        for (var searchingBuildingIndex in that.data.curentMap) {
                            if (searchingBuildingIndex !== curentBuildingIndex) {
                                var searchingCurent = that.data.curentMap[searchingBuildingIndex];
                                var searchingSchedule = that.data.scheduleMap[searchingBuildingIndex];
                                if (searchingCurent &&
                                    searchingSchedule &&
                                    (searchingCurent.btype === 0) &&
                                    (searchingSchedule === curentBuilding.building)) {
                                    // Found!
                                    found = true;
                                    that.utils.errorMsg.display("Moving this building");
                                    jQuery.ajax({
                                        url: 'includes/mBu.php',
                                        type: 'POST',
                                        aysnc: false,
                                        data: "a=" + curentBuildingIndex + "&b=" + searchingBuildingIndex + "&c=" + that.data.id
                                    });
                                    break;
                                }
                            }
                        }
                        if (!found) {
                            that.utils.errorMsg.display("Error. Don't know where to move this building. Please report the bug with current and plan maps");
                        }
                    } else if (curentBuilding.toAdd) {
                        toAdd = that.data.scheduleMap[selectedBuilding];
                    } else if (curentBuilding.btype === 0) {
                        // Have click on empty & nothing planed
                        if (that.data.curentMap.filter(function (item) {
                                return item.building && item.building.buildable
                            }).length < 50) {
                            // Adding a cottage
                            toAdd = that.BUILDINGS._DETAILS.cottage;
                        } else if (that.addedData.pureNavy) {
                            // Adding a barrack
                            toAdd = that.BUILDINGS._DETAILS.barracks;
                        } else if (that.addedData.pureTrade) {
                            // Adding a forum
                            toAdd = that.BUILDINGS._DETAILS.marketplace;
                        }
                    }
                    if (toAdd) {
                        if (toAdd.shortcut) {
                            that.utils.errorMsg.display("Creating " + toAdd.name);
                            var eventData = {
                                type: "keypress",
                                keyCode: toAdd.shortcut.charCodeAt()
                            };
                            $("body").trigger(eventData);
                        } else {
                            that.utils.errorMsg.display("Error. Don't know how to creat " + toAdd.name + ". Please report the bug");
                        }
                    }
                }
            }
        };

        // Toggle automode
        that.toggleAutoMode = function () {
            that.data.autoModeActivated = !that.data.autoModeActivated;
            that.onCityDataUpdated(false);
            that.utils.errorMsg.display("Auto build mode " +
                (that.data.autoModeActivated ? "activated" : "disactivated"));
        }
        return that;
    }();

    // Adding buttons once game loaded
    var waitForGameLoaded = function () {
        if ($("#incAttacksDiv").length > 0) {
            console.log("Game loaded.");

            // Add'on starting.
            if ($("#" + LIO_DIV).length > 0) {
                // Removing current
                $("#" + LIO_DIV).remove();
            }

            // Buttons for building in left bar
            $("#incAttacksDiv").before(
                '<div id="' + LIO_DIV + '" class="commandinndiv">' +
                '<div style="margin-left: 8px;margin-top: 10px;font-size: 12px;">Set a map to activate functionnality</div>' +
                '<div style="display:none;">' +
                '<button ' + CLASS_AND_CSS_FIRST_BUTTON + '>Info</button>' +
                '<select title="Remove building or resource" ' + CLASS_AND_CSS_NEXT_BUTTON + '><option>Clear</option></select>' +
                '<select id="' + SELECT_CITY_MOVE_ID + '" title="Move a building " ' + CLASS_AND_CSS_NEXT_BUTTON + '><option>Move</option></select>' +
                '<select id="' + SELECT_CITY_ADD_ID + '" title="Add a building" ' + CLASS_AND_CSS_NEXT_BUTTON + '><option>Add</option></select>' +
                '</div></div>');

            // TODO: add buildtooltip tooltipstered & tooltip for BUTTON_CITY_CLEAR_ID
            $(BUTTON_CITY_INFO_SELECTOR).click(function () {
                LIO.buildManager.toggleAutoMode();
            });

            $("#city_map").click(function () {
                LIO.buildManager.processBuilding();
            });
        } else {
            setTimeout(waitForGameLoaded, 1000);
        }

        console.log("Lionnel0Script loaded.");
    };
    waitForGameLoaded();

    // Adding for clearing the chat
    var waitForChat = function () {
        if ($("#clbleft").length > 0) {

            // Adding clear button to chat
            $("#clbleft").append('<button id="clbclear" class="greenb">Clear</button>');
            $("#clbclear").click(function () {
                $("#chatDisplay").html(""); // World
                $("#chatDisplaya").html(""); // Alliance
                $("#chatDisplayo").html(""); // Officer
                $("#chatDisplayw").html(""); // Wisper
            });
            $("#chlinkbuts").attr("style", $("#chlinkbuts").attr("style") + ";height: 125px;");
            $("#clbright").attr("style", "height: 120px;");
        } else {
            setTimeout(waitForChat, 1000);
        }
    };
    waitForChat();

    // Overriding some responses
    setTimeout(function () {
        (function (open) {
            XMLHttpRequest.prototype.open = function () {
                this.addEventListener("readystatechange", function () {
                    if (this.readyState == 4) {
                        var url = this.responseURL;
                        var cdata;
                        if (url.indexOf('gC.php') != -1) {
                            // Changing city response.
                            try {
                                cdata = JSON.parse(this.response);
                                LIO.buildManager.parseCurentCityData(cdata);
                            } catch (e) {
                                console.error(e);
                            }
                        } else if (url.indexOf('poll2.php') != -1) {
                            // Poll2 response: Updating curent city
                            try {
                                var poll2 = JSON.parse(this.response);
                                cdata = poll2.city;
                                LIO.buildManager.parseCurentCityData(cdata);
                            } catch (e) {
                                console.error(e);
                            }
                        }
                    }
                }, false);
                open.apply(this, arguments);
            };
        })(XMLHttpRequest.prototype.open);
    }, 4000);

})();