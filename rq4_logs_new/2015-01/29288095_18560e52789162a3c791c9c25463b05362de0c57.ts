/// <reference path="typings/jquery/jquery.d.ts" />
// /*jslint browser: true*/
/*global $, jQuery, alert*/

/**
 * Created by Stephael on 11.01.2015.
 */

var steps = [];
var currStep = 0;

function tableCreate(arr) {
    "use strict";
    var td,
        tr,
        body = document.body,
        tbl = document.createElement('table'),
        i,
        j;

    for (i = 0; i < 9; i += 1) {
        tr = tbl.insertRow();
        for (j = 0; j < 9; j += 1) {
            td = tr.insertCell();
            td.innerHTML = parseLine(arr[i][j]);
        }
    }
    body.appendChild(tbl);
}

function deleteTableAndCreateNew() {
    "use strict";
    $("#curr").text(currStep + 1);
    $("#total").text(steps.length);

    $("table").remove();
    tableCreate(steps[currStep]);
}

function getCell(row : number, col : number) {
    "use strict";
    return Math.floor(col / 3) + 3 * Math.floor(row / 3);
}

function countFree(row : number, col : number, arr) {
    "use strict";
    var offsetCol,
        offsetRow,
        cell,
        values = new Array(3),
        i,
        j;

    for (i = 0; i < values.length; i += 1) {
        values[i] = new Array(10);
        for (j = 0; j < values[i].length; j += 1) {
            values[i][j] = 0;
        }
    }

    for (i = 0; i < 9; i += 1) {
        arr[row][i].forEach(
            function (currentValue, index, array) {
                if (array.length > 1) {
                    values[0][currentValue] += 1;
                }
            }
        );
        arr[i][col].forEach(
            function (currentValue, index, array) {
                if (array.length > 1) {
                    values[1][currentValue] += 1;
                }
            }
        );
    }
    for (i = 0; i < 3; i += 1) {
        for (j = 0; j < 3; j += 1) {

            cell = getCell(row, col);
            offsetRow = 3 * Math.floor(cell / 3);
            offsetCol = 3 * (cell % 3);
            arr[i + offsetRow][j + offsetCol].forEach(function (currentValue, index, array) {
                if (array.length > 1) {
                    values[2][currentValue] += 1;
                }
            });
        }
    }
    return values;
}

// Validations

function validateRow(row : number) {
    "use strict";
    var fieldVal,
        values = new Array(10),
        i;

    for (i = 0; i < values.length; i += 1) {
        values[i] = false;
    }

    for (i = 0; i < 9; i += 1) {
        fieldVal = steps[0][row][i];

        if (fieldVal.length === 1) {
            if (values[fieldVal[0]] === false) {
                values[fieldVal[0]] = true;
            } else {
                alert("Die " + fieldVal[0] + " ist doppelt Eingetragen in Zeile " + (row + 1));
            }
        }
    }
}

function validateColumn(col : number) {
    "use strict";
    var values = new Array(10),
        i,
        fieldVal;

    for (i = 0; i < values.length; i += 1) {
        values[i] = false;
    }

    for (i = 0; i < 9; i += 1) {
        fieldVal = steps[0][i][col];

        if (fieldVal.length === 1) {
            if (values[fieldVal[0]] === false) {
                values[fieldVal[0]] = true;
            } else {
                alert("Die " + fieldVal[0] + " ist doppelt Eingetragen in Spalte " + (col + 1));
            }
        }
    }
}

function validateCell(cell : number) {
    "use strict";
    var values = new Array(10),
        i,
        j,
        offsetRow,
        offsetCol,
        fieldVal;

    for (i = 0; i < values.length; i += 1) {
        values[i] = false;
    }

    for (i = 0; i < 3; i += 1) {
        for (j = 0; j < 3; j += 1) {
            offsetRow = 3 * Math.floor(cell / 3);
            offsetCol = 3 * (cell % 3);
            fieldVal = steps[0][i + offsetRow][j + offsetCol];

            if (fieldVal.length === 1) {
                if (values[fieldVal[0]] === false) {
                    values[fieldVal[0]] = true;
                } else {
                    alert("Die " + fieldVal[0] + " ist doppelt Eingetragen in Zelle " + (cell + 1));
                }
            }
        }
    }
}


function valuesInCell(cell : number, arr) {
    "use strict";
    var values = new Array(9),
        i,
        j,
        offsetRow,
        offsetCol,
        fieldVal;

    for (i = 0; i < 3; i += 1) {
        for (j = 0; j < 3; j += 1) {
            offsetRow = 3 * Math.floor(cell / 3);
            offsetCol = 3 * (cell % 3);
            fieldVal = arr[i + offsetRow][j + offsetCol];

            if (fieldVal.length === 1) {
                values[fieldVal[0]] = fieldVal[0];
            }
        }
    }
    return values;
}

function valuesInRow(row : number, arr) {
    "use strict";
    var values = new Array(9),
        i,
        fieldVal;

    for (i = 0; i < 9; i += 1) {
        fieldVal = arr[row][i];

        if (fieldVal.length === 1) {
            values[fieldVal[0]] = fieldVal[0];
        }
    }
    return values;
}

function valuesInCol(col : number, arr) {
    "use strict";
    var values = new Array(9),
        i,
        fieldVal;

    for (i = 0; i < 9; i += 1) {
        fieldVal = arr[i][col];

        if (fieldVal.length === 1) {
            values[fieldVal[0]] = fieldVal[0];
        }
    }
    return values;
}

function parseLine(arr) {
    "use strict";
    var result = "",
        i;
    for (i = 0; i < arr.length; i += 1) {
        result += arr[i];
        if (i % 3 === 2 && i > 0) {
            result += "<br/>";
        } else if (i < arr.length - 1) {
            result += ",";
        }
    }
    return result;
}

function parseInput() {
    "use strict";
    var input = [],
        line,
        i;
    $("input").each(function (index) {
        if (index % 9 === 0) {
            line = [];
            input.push(line);
        }
        var entry = $(this).val();
        if (entry !== "") {
            line.push([parseInt(entry, 10)]);
        } else {
            line.push([]);
        }
    });
    steps[0] = input;

    for (i = 0; i < 9; i += 1) {
        validateRow(i);
        validateColumn(i);
        validateCell(i);
    }
}

function isInRow(element : number, row : number, arr) {
    "use strict";
    return valuesInRow(row, arr).indexOf(element) !== -1;
}

function isInColumn(element : number, column : number, arr) {
    "use strict";
    return valuesInCol(column, arr).indexOf(element) !== -1;
}

function isInCell(element : number, cell : number, arr) {
    "use strict";
    return valuesInCell(cell, arr).indexOf(element) !== -1;
}

// called from html

function reducePossibleEntries() {
    var changed = false,
        i,
        j,
        length,
        step = steps.length - 1,
        copyArray;

    for (i = 0; i < 9; i += 1) {
        for (j = 0; j < 9; j += 1) {
            length = steps[step][i][j].length;
            if (length > 1) {
                steps[step][i][j] = steps[step][i][j].filter(
                    function (element) {
                        var inRow = isInRow(element, i, steps[step]),
                            inCol = isInColumn(element, j, steps[step]),
                            inCell = isInCell(element, getCell(i, j), steps[step]);
                        return !inRow && !inCol && !inCell;
                    }
                );
                if (length !== steps[step][i][j].length) {
                    changed = true;
                }
                if (steps[step][i][j].length === 1) {
                    copyArray = steps[step];
                    step += 1;
                    steps[step] = JSON.parse(JSON.stringify(copyArray));
                }
            }
        }
    }
    return changed;
}

function onlyPossibleEntry() {
    "use strict";
    var nums,
        copyArray,
        i,
        j,
        changed = false,
        step = steps.length - 1;

    for (i = 0; i < 9; i += 1) {
        for (j = 0; j < 9; j += 1) {
            if (steps[step][i][j].length !== 1) {
                nums = countFree(i, j, steps[step]);

                nums.forEach(function (value, index, array) {
                    var innerCount = 0,
                        innerValues = []; 
                    value.forEach(function (val, ind) {

                        if (val === 1) {
                            if (steps[step][i][j].indexOf(ind) !== -1) {
                                innerCount += 1;
                                innerValues.push(ind);
                            }
                        }
                    });
                    if (innerCount === 1) {
                        changed = true;
                        steps[step][i][j] = innerValues;
                        copyArray = steps[step];
                        step += 1;
                        steps[step] = JSON.parse(JSON.stringify(copyArray));
                    }
                });
            }
            if (changed) {
                break;
            }
        }
        if (changed) {
            break;
        }
    }
    return changed;
}

function unionInRow(row : number, setSize : number, arr) {

    var i,
        j,
        length,
        positionLength;

    for (i = 0; i < 9; i += 1) {
        length = arr[row][i].length;
        if (length <= setSize) {
            for (j = 0; j < 9; j += 1) {
                if (i !== j) {
                    positionLength = arr[row][j].length;
                    if (positionLength <= setSize) {


                    }
                }
            }
        }
    }
}


function reduceByUnion() {
    "use strict";

    var i,
        j,
        step = steps.length - 1;

    for (i = 0; i < 9; i += 1) {
        for (j = 2; j < 9; j += 1)
        unionInRow(i, j, steps[step]);
        //unionInColumn(i, j, steps[step]);
        //unionInCell(i, j, steps[step]);
    }

}

function solve() {
    "use strict";
    var i,
        j,
        changed = true;
    steps = [];
    parseInput();

    //fill with default values
    for (i = 0; i < 9; i += 1) {
        for (j = 0; j < 9; j += 1) {
            if (steps[0][i][j].length === 0) {
                steps[0][i][j] = [1, 2, 3, 4, 5, 6, 7, 8, 9];
            }
        }
    }

    while (changed) {
        changed = reducePossibleEntries();
        if (changed === false) {
            changed = onlyPossibleEntry();
        }
        if (changed === false) {
            //changed = reduceByUnion();
        }
    }
    deleteTableAndCreateNew();
}

function reset() {
    "use strict";
    steps = [];
    currStep = 0;

    $("#curr").text(0);
    $("#total").text(0);

    $("table").remove();
    $("input").each(function () {
        $(this).val("");
    });
}

function displayFirstStep() {
    "use strict";
    if (steps.length === 0) {
        return;
    }
    currStep = 0;
    deleteTableAndCreateNew();
}

function displayNextStep() {
    "use strict";
    if (steps.length === 0) {
        return;
    }
    currStep = Math.min(currStep + 1, steps.length - 1);
    deleteTableAndCreateNew();
}

function displayPreviousStep() {
    "use strict";
    if (steps.length === 0) {
        return;
    }
    currStep = Math.max(currStep - 1, 0);
    deleteTableAndCreateNew();
}

function displayLastStep() {
    "use strict";
    if (steps.length === 0) {
        return;
    }
    currStep = steps.length - 1;
    deleteTableAndCreateNew();
}

$(document).keydown(function(evt) {
    "use strict";
    switch (evt.keyCode) {
    case 37:    // left
        displayPreviousStep();
        break;
    case 39:    // right
        displayNextStep();
        break;
    case 13:
        solve();
        break;
    case 82:
        reset();
        break;
    default:
        break;
    }
});