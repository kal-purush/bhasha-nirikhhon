(function () {
    "use strict";

    var activation = Windows.ApplicationModel.Activation;
    var roaming=Windows.Storage.ApplicationData.current.roamingSettings;
    

    window.onload = function () {
        document.getElementById("myBtn").onclick = function () {
            window.location.href = "ms-appx:///newPage.html";
        }
        //document.getElementById("userName").onchange = function (evt) {
        //    roaming.values["UserName"] = evt.srcElement.value;
        //}
    }
    

    Windows.UI.WebUI.WebUIApplication.addEventListener("activated", function (args) {
        if (args.detail[0].kind === activation.ActivationKind.launch) {
            if (roaming.values["currentUri"]) {
                window.location.href = roaming.values["currentUri"];
            }
        }
    });

    Windows.UI.WebUI.WebUIApplication.addEventListener("suspending", function (args) {
        roaming.values["currentUri"] = window.location.href;
        roaming.values["UserName"] = evt.srcElement.value;
        //save the other information of the page here
    });

    Windows.UI.WebUI.WebUIApplication.addEventListener("resuming", function (args) {
        var roam = Windows.Storage.ApplicationData.current.roamingSettings;
        if (roam) {
            if (roam["currentUri"])
            {
                window.location.href = roam["currentUri"];
            }
        }
    }, false);


})();