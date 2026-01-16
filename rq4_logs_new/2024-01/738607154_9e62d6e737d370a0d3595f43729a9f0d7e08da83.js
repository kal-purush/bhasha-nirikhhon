jQuery(document).ready(function($) {
    $('#adzuna-job-search-form').submit(function(event) {
        event.preventDefault();
        var title = $("input[name='title']").val();
        $("#results").html('<progress class="progress is-small is-primary" max="100">15%</progress>');

        $.ajax({
            url: adzunaAjax.ajaxurl,
            type: 'POST',
            data: {
                action: 'adzuna_search_jobs',
                title: title
            },
            success: function(response) {
                $("#results").html(response);
            },
            error: function() {
                $("#results").html("Error occurred while fetching jobs.");
            }
        });
    });
});