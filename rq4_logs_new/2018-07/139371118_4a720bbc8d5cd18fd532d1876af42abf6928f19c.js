var SERVER = "http://localhost:3000/";

function doAjaxCall(method, cmd, params, fcn) {
    $.ajax(
        SERVER + cmd,
        {
            type: method,
            processData: true,
            data: params,
            dataType: "jsonp",
            success: function (result) {
                fcn(result)
            },
            error: function (jqXHR, textStatus, errorThrown) {
                alert("Error1: " + errorThrown);
            }
        }
    );
}

//on page load attach button handler 
$(() => {
    $("#theForm").submit(function () {
        var formData = $('#theForm').serializeArray();
        console.log( formData);
        var tmp = JSON.stringify(formData);
        doAjaxCall("GET", "data", { formData: tmp }, function (res) {
            console.log("success");
        });
    });
});