export module resources {
    $(document).ready(function() {
        $(".container-groups").accordion({
            "heightStyle": "content"
        });
        $(".container-courses").tabs();
    });
}