function orderFromCodeReviewController($scope, $http, $q) {

    $('#loaderImage').show();

    $scope.sampleJSONClass = {};
    $scope.sampleJSONTrigger = {};
    $scope.sampleJSONPages = {};
    oboe('/utilities/longProcessStream')
        .done(function(data) {
            if (data == 'LastByte') {
                localStorage.setItem('apexReview', JSON.stringify($scope.sampleJSONClass));
                localStorage.setItem('triggerReview', JSON.stringify($scope.sampleJSONTrigger));
            }
            var dataFromServer = data.pmdStructureWrapper;
            if (Object.keys(dataFromServer)[0].endsWith('.cls')) {
                $scope.sampleJSONClass[Object.keys(dataFromServer)[0]] = Object.values(dataFromServer)[0];
            }
            if (Object.keys(dataFromServer)[0].endsWith('.trigger')) {
                $scope.sampleJSONTrigger[Object.keys(dataFromServer)[0]] = Object.values(dataFromServer)[0];
            }
            if (Object.keys(dataFromServer)[0].endsWith('.page')) {
                $scope.sampleJSONPages[Object.keys(dataFromServer)[0]] = Object.values(dataFromServer)[0];
            }
            $scope.$apply();
            $('#loaderImage').hide();
        })
        .fail(function() {
            console.log('error');
        });

    $scope.selectedClassErrDetails = [];
    $scope.selectedClassName = "";
    $scope.showErrorDetailsClass = function(classNameKey) {
        $scope.selectedClassName = classNameKey;
        $scope.selectedClassErrDetails = $scope.sampleJSONClass[classNameKey].pmdStructures;
        $('#myModal').modal('show', testAnim('zoomIn'));
    };

    $scope.showErrorDetailsTrigger = function(classNameKey) {
        $scope.selectedClassName = classNameKey;
        $scope.selectedClassErrDetails = $scope.sampleJSONTrigger[classNameKey].pmdStructures;
        $('#myModal').modal('show', testAnim('zoomIn'));
    };

    $scope.showErrorDetailsPage = function(classNameKey) {
        $scope.selectedClassName = classNameKey;
        $scope.selectedClassErrDetails = $scope.sampleJSONPages[classNameKey].pmdStructures;
        $('#myModal').modal('show', testAnim('zoomIn'));
    };

    $scope.selectedClassDupErrDetails = [];
    $scope.showDuplicateDetails = function(eachDuplicateData) {
        $scope.selectedClassDupErrDetails = eachDuplicateData;
        $('#myModalDuplicates').modal('show', testAnim('zoomIn'));

    };

    $scope.logThisDefect = function() {
        var selClassName = $scope.selectedClassName;
        var selClassErrorDetails = $scope.selectedClassErrDetails;
        console.log("selClassName.........");
        console.log(selClassName);
        console.log(selClassErrorDetails);
    };

}

function testAnim(x) {
    $('.modal .modal-dialog').attr('class', 'modal-dialog  ' + x + '  animated');
}


function parseURLParams(url) {
    var queryStart = url.indexOf("?") + 1,
        queryEnd = url.indexOf("#") + 1 || url.length + 1,
        query = url.slice(queryStart, queryEnd - 1),
        pairs = query.replace(/\+/g, " ").split("&"),
        parms = {},
        i, n, v, nv;

    if (query === url || query === "") return;

    for (i = 0; i < pairs.length; i++) {
        nv = pairs[i].split("=", 2);
        n = decodeURIComponent(nv[0]);
        v = decodeURIComponent(nv[1]);

        if (!parms.hasOwnProperty(n)) parms[n] = [];
        parms[n].push(nv.length === 2 ? v : null);
    }
    return parms;
}

function searchFunction() {
    var input, filter, table, tr, td, i;
    input = document.getElementById("searchBar");
    filter = input.value.toUpperCase();
    table = document.getElementById("errorTable");
    tr = table.getElementsByTagName("tr");
    for (i = 0; i < tr.length; i++) {
        td = tr[i].getElementsByTagName("td")[2];
        if (td) {
            if (td.innerHTML.toUpperCase().indexOf(filter) > -1) {
                tr[i].style.display = "";
            } else {
                tr[i].style.display = "none";
            }
        }
    }
}

function resetSearchContent() {
    var content = document.getElementById("searchBar");
    content.value = null;
}