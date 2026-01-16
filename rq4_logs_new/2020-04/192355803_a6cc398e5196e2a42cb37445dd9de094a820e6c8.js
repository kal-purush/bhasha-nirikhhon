/**
 * All the User Account things.
 */
$(document).on("click", ".delete-acc-alert", function(e) {
    bootbox.confirm({
        title: "Confirm",
        message: "Are your sure you want to delete your account? <br> <p class='text-muted'>Note : This cannot be undone.</p>",
        backdrop: true,
        centerVertical: true,
        callback: function(result) { if(result) deleteAcc() }
    });
});

function deleteAcc() {
	$.get("/user/account/delete/" + $('#uid').val(), function(data, textStatus, jqXHR) {
		if(data){
			window.location.href = "/logout"
		}
	});
}