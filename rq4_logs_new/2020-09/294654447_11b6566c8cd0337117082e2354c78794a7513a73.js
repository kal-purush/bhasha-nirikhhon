function getMemberList() {
    let members = ['Member One', 'Member Two', 'Member Three', 'Member Four', 'Member Five', 'Member Six', 'Member Seven', 'Member Eight', 'Member Nine', 'Member Ten', 'Member Eleven'];
    return members;
}

function getConfig() {
    var members = getMemberList();
    var pairing_matrix = {
        "members": members,
        "matrix_x": "B",
        "matrix_y": 7,
        "status_x": "B",
        "status_y": members.length + 10,
        "iteration_board_x": "B",
        "iteration_board_y": (2 * members.length) + 15,
        "red_zone": 3,
        "yellow_zone": 1
    }
    return pairing_matrix;
}

function getColorCode(color) {
    var colors = {
        "red": "#FF0000",
        "green": "#00FA9A",
        "yellow": "#FFDD88",
        "white": "#FFFFFF"
    }
    return colors[color];
}
function increaseCharacter(c, count) {
    return String.fromCharCode(c.charCodeAt(0) + count);
}
function intialize_names_dropdowns() {
    var helperText = "Please select name from given List";
    // first drop down
    var cellOne = SpreadsheetApp.getActive().getRange('A3');
    var ruleOne = SpreadsheetApp.newDataValidation().requireValueInList(getMemberList(), true).setAllowInvalid(false).setHelpText(helperText).build();
    cellOne.setDataValidation(ruleOne);
    // second drop down
    var cellTwo = SpreadsheetApp.getActive().getRange('B3');
    var ruleTwo = SpreadsheetApp.newDataValidation().requireValueInList(getMemberList(), true).setAllowInvalid(false).setHelpText(helperText).build();
    cellTwo.setDataValidation(ruleTwo);
}

function createMembersListByYAxis(x_axis, y_axis) {
    var config = getConfig();
    var x = increaseCharacter(x_axis, -1);
    var y = y_axis;

    var members = config['members'];
    for (let i = 0; i < members.length; i++) {
        var index_y = (i + y).toString(10);
        var cell = SpreadsheetApp.getActive().getRange(x.concat(index_y));
        cell.setValue(members[i]);
    }
}

function createMembersListByXAxis(x_axis, y_axis) {
    var config = getConfig();
    var members = config['members'];
    x = x_axis;
    y = y_axis - 1;
    for (let i = 0; i < members.length; i++) {
        var index_x = increaseCharacter(x, i);
        var cell = SpreadsheetApp.getActive().getRange(index_x.concat(y));
        cell.setValue(members[i]);
    }
}

function initialize_matrix_board() {
    var config = getConfig();
    var cell = SpreadsheetApp.getActive().getRange("A".concat(config['matrix_y'] - 2));
    cell.setValue("Status Board-Values are in numbers (e.g 0.5,1,...) -No need of manual updation")
    cell.setFontColor(getColorCode('red'))
    createMembersListByXAxis(config['matrix_x'], config['matrix_y']);
    createMembersListByYAxis(config['matrix_x'], config['matrix_y']);
}

function initialize_status_board() {
    var config = getConfig();
    var cell = SpreadsheetApp.getActive().getRange("A".concat(config['status_y'] - 2));
    cell.setValue("Current Pair Status (No need of manual updation)")
    cell.setFontColor(getColorCode('red'))
    createMembersListByYAxis(config['status_x'], config['status_y']);
}

function initialize_iteration_board() {
    var config = getConfig();
    var cell = SpreadsheetApp.getActive().getRange("A".concat(config['iteration_board_y'] - 2));
    cell.setValue("Current Iteration (iteration #NUMBER) (No need of manual edits)")
    cell.setFontColor(getColorCode('red'))
    createMembersListByXAxis(config['iteration_board_x'], config['iteration_board_y']);
    createMembersListByYAxis(config['iteration_board_x'], config['iteration_board_y']);
}

function create_matrix() {
    intialize_names_dropdowns();
    initialize_matrix_board();
    initialize_status_board();
    initialize_iteration_board();
}


function getStartXofMatrix() {
    return getConfig()['matrix_x'];
}

function getStartYofMatrix() {
    return getConfig()['matrix_y'];
}

function getStartXofStatus() {
    return getConfig()['status_x'];
}

function getStartYofStatus() {
    return getConfig()['status_y'];
}

function getStartYofIterationBoard() {
    return getConfig()['iteration_board_y'];
}

function getStartXofIterationBoard() {
    return getConfig()['iteration_board_x'];
}


function redZone() {
    return getConfig()['red_zone'];
}

function yellowZone() {
    return getConfig()['yellow_zone'];
}

function isValidNumber(val) {
    if (val == "") { return false; }
    if (isNaN(val) == true) { return false; }
    return true;
}

function deleteLastPairCell(memberOne) {
    let map = new Map();
    let members = getMemberList();
    let statusY = getStartYofStatus();
    for (i = 0; i < members.length; i++) {
        map.set(members[i], statusY);
        statusY = statusY + 1;
    }
    let memberTwo = SpreadsheetApp.getActiveSheet().getRange(getStartXofStatus() + "" + map.get(memberOne)).getValue();
    let range = getRangeOf(memberOne, memberTwo);
    //Browser.msgBox(range);
    try {
        SpreadsheetApp.getActiveSheet().getRange(range).clear();
    } catch (err) { }
}

function updateIterationBoard(memberOne, memberTwo, value) {
    let map = new Map();
    let members = getMemberList();
    let startX = getStartXofIterationBoard();
    for (i = 0; i < members.length; i++) {
        map.set(members[i], startX);
        startX = String.fromCharCode(startX.charCodeAt() + 1)
    }
    let mapX = new Map();
    let startY = getStartYofIterationBoard();
    for (i = 0; i < members.length; i++) {
        mapX.set(members[i], startY);
        startY = startY + 1;
    }

    let rangeOneX = mapX.get(memberOne);
    let rangeOneY = map.get(memberTwo);

    let rangeTwoX = mapX.get(memberTwo);
    let rangeTwoY = map.get(memberOne);

    let range = rangeTwoY + "" + rangeTwoX;
    if (rangeOneY > rangeTwoY) { range = rangeOneY + "" + rangeOneX; }

    let oldvalue = SpreadsheetApp.getActiveSheet().getRange(range).getValue();
    let newValue = oldvalue + value;
    if (newValue <= 0) { newValue = 0; }
    SpreadsheetApp.getActiveSheet().getRange(range).setValue(newValue);
}

function updateStatus(memberOne, memberTwo) {
    let map = new Map();
    let members = getMemberList();
    let statusY = getStartYofStatus();
    for (i = 0; i < members.length; i++) {
        map.set(members[i], statusY);
        statusY = statusY + 1;
    }
    try {
        SpreadsheetApp.getActiveSheet().getRange(getStartXofStatus() + "" + map.get(memberOne)).setValue(memberTwo);
        SpreadsheetApp.getActiveSheet().getRange(getStartXofStatus() + "" + map.get(memberTwo)).setValue(memberOne);
    }
    catch (err) { }
}

function getRangeOf(memberOne, memberTwo) {
    let map = new Map();
    let members = getMemberList();
    let startX = getStartXofMatrix();
    for (i = 0; i < members.length; i++) {
        map.set(members[i], startX);
        startX = String.fromCharCode(startX.charCodeAt() + 1)
    }

    let mapX = new Map();
    let startY = getStartYofMatrix();
    for (i = 0; i < members.length; i++) {
        mapX.set(members[i], startY);
        startY = startY + 1;
    }

    let rangeOneX = mapX.get(memberOne);
    let rangeOneY = map.get(memberTwo);

    let rangeTwoX = mapX.get(memberTwo);
    let rangeTwoY = map.get(memberOne);

    if (rangeOneY > rangeTwoY) { return rangeOneY + "" + rangeOneX; }
    return rangeTwoY + "" + rangeTwoX;
}

function getInputs() {
    let inputs = [0];
    one = SpreadsheetApp.getActiveSheet().getRange('A3').getValue();
    two = SpreadsheetApp.getActiveSheet().getRange('B3').getValue();
    if (one.length == 0 || two.length == 0) { Browser.msgBox("Please, Enter valid input"); return inputs }
    inputs = [one, two];
    return inputs;
}

function shellUpdation(newValue, range) {
    if (newValue <= 0) {
        SpreadsheetApp.getActiveSheet().getRange(range).clearContent();
        SpreadsheetApp.getActiveSheet().getRange(range).setBackground(getColorCode('white'));
    }
    else if (newValue <= yellowZone()) {
        SpreadsheetApp.getActiveSheet().getRange(range).setBackground(getColorCode('green'));
    }
    else if (newValue > redZone()) {
        SpreadsheetApp.getActiveSheet().getRange(range).setBackground(getColorCode('red'));
    }
    else {
        SpreadsheetApp.getActiveSheet().getRange(range).setBackground(getColorCode('yellow'));
    }
}


function addDays() {
    inputNames = getInputs()
    if (inputNames.length == 2) {
        one = inputNames[0];
        two = inputNames[1];
        let range = getRangeOf(one, two);
        let val = parseFloat(Browser.inputBox("How many pairing days you want to add for " + one + " and  " + two + " ?  (this will be added with old value)"));
        if (!isValidNumber(val)) { Browser.msgBox("Please enter valid number"); return; }
        if (val < 0.5) { return; }
        let value = SpreadsheetApp.getActiveSheet().getRange(range).getValue();
        let newValue = value + val;
        deleteLastPairCell(one);
        deleteLastPairCell(two);
        SpreadsheetApp.getActiveSheet().getRange(range).setValue(newValue);
        updateStatus(one, two);
        shellUpdation(newValue, range);
        updateIterationBoard(one, two, val);

    }
}


function reduceDays() {
    inputNames = getInputs()
    if (inputNames.length == 2) {
        one = inputNames[0];
        two = inputNames[1];
        let range = getRangeOf(one, two);
        let val = parseFloat(Browser.inputBox("How many pairing days you want to reduce for " + one + " and  " + two + " ?  (this will be subtracted from old value)"));
        if (!isValidNumber(val)) { Browser.msgBox("Please enter valid number"); return; }
        if (val < 0.5) { return; }
        let value = SpreadsheetApp.getActiveSheet().getRange(range).getValue();
        let newValue = value - val;
        SpreadsheetApp.getActiveSheet().getRange(range).setValue(newValue);
        shellUpdation(newValue, range);
        updateIterationBoard(one, two, -1 * val);
    }
}
