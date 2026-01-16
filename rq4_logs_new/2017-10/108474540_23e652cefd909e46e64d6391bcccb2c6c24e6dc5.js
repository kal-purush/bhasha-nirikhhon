var educationSelect = document.getElementById('educationSelect');
var citiesSelect = document.getElementById('citiesSelect');

educationSelect.addEventListener('change',
    function () {
        updateUsersInfoHtmlTable();
    }
);

citiesSelect.addEventListener('change',
    function () {
        updateUsersInfoHtmlTable();
    }
);

function updateUsersInfoHtmlTable() {
    var educationNamesToShow = [];

    for (var i = 0; i < educationSelect.length; i++) {
        if (educationSelect[i].selected) {
            educationNamesToShow.push(educationSelect[i].innerText.trim());
        }
    }

    var citiesNamesToShow = [];

    for (var i = 0; i < citiesSelect.length; i++) {
        if (citiesSelect[i].selected) {
            citiesNamesToShow.push(citiesSelect[i].innerText.trim());
        }
    }

    var table = document.getElementById("usersInfoTable");
    // i = 1, а не 0, так как первую строку нужно пропустить - она состоит из <th>
    for (var i = 1, row; row = table.rows[i]; i++) {
        var educationColumn = 1;
        var rowEducationName = row.cells[educationColumn].innerText.trim();
        var citiesColumn = 2;
        var rowCitiesNames = row.cells[citiesColumn].innerText.split(',');
        row.style.display = include(educationNamesToShow, rowEducationName) &&
            checkCitiesNames(citiesNamesToShow, rowCitiesNames) ? 'table-row' : 'none';
    }
}

function checkCitiesNames(citiesNamesToShow, rowCitiesNames) {
    for (var i = 0; i < rowCitiesNames.length; i++) {
        if (include(citiesNamesToShow, rowCitiesNames[i].trim())) {
            return true;
        }
    }
    return false;
}

function include(arr, obj) {
    return arr.indexOf(obj) != -1;
}