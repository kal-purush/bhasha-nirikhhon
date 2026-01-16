$(document).ready()
{
    var $a = $("a[data-delete]");

    $a.click(function (e) {
        e.preventDefault();

        var $a = $(this);
        var token = $(this).attr("data-token");
        var url = $(this).attr("href");
        if (confirm("Sur ???")) {

            // fetch = requete (jusquau premier then)
            fetch(url, {
                method: "DELETE",
                headers: {
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({"_token": token})
            }).then(
                //    recup la reponse json
                response => response.json()
            ).then(data => {
                // fais des choses avec la response
                if (data.success)
                    $a.parent().remove()
                else
                    alert(data.error)

            }).catch(e => alert(e))
            // alert(url);
        }
    })
}