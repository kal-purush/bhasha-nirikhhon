var isFileDeleted = "@ViewBag.IsDeleted";
var key = "";
var urlPrefix = "https://bold-meeting-recording.s3.us-east-2.amazonaws.com/";
var deleteData = "";
var customerName = "";
var fileName = "";

$(document).ready(function () {
    $('#list-item-nav').removeClass("show").addClass("hide");
    $('#upload-nav').removeClass("hide").addClass("show");
    $(document).on("click", "#upload-nav", function () {
        $('#list-item-nav').removeClass("hide").addClass("show");
        $('#upload-nav').removeClass("show").addClass("hide");
    });

    $(document).on("click", "#delete-confirm-btn", function () {
        $("#delete-confirm-dialog").removeClass("show").addClass("hide");
        $.ajax({
            url: `/delete-file?key=${key}`,
            type: 'POST',
            cache: false,
            contentType: false,
            processData: false,
            success: function (result) {
                if (result.result) {
                    cuteToast({
                        type: "success",
                        message: "File deleted successfully",
                        timer: 5000
                    })
                    location.reload();
                }
                else {
                    cuteToast({
                        type: "error",
                        message: result.message,
                        timer: 5000
                    })
                }
            }
        });
    });

    $(document).on("click", "#delete-btn", function () {
        var resource = key.split('/');
        if (resource.length == 2) {
            customerName = resource[0];
            fileName = resource[1];
        }
        else {
            customerName = Unspecified;
            fileName = resource[0];
        }
        deleteData = `<b>Customer:</b> ${customerName}<br/><br/>` +
            `<b>File:</b> <a href='${urlPrefix}${key}'>${fileName}</a>`;
        $('#delete-confirm-dialog').modal('handleUpdate')
        $("#delete-data").html(deleteData)
        $("#delete-confirm-trigger").click();
    });
});

function reloadGrid() {
    location.reload();
}

function rowSelected(args) {
    var row = args.row;
    key = row.cells[0].innerText;
    fnCopyClientCredentials(key);
}

function fnCopyClientCredentials(key) {
    var url = urlPrefix + key; 
    navigator.clipboard.writeText(url);   
}