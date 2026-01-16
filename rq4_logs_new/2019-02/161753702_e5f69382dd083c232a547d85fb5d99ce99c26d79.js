
    $(function () {
        $("#minus_spring-header").click(function () {
            $(".spring").hide();
            $(".colSpring").show();
            $("#checkSpring").prop('checked', false);

        });
    });

    $(function () {
        $("#minus_htmlcss-header").click(function () {
            $(".htmlcss").hide();
            $(".colHtml").show();
            $("#checkHtml").prop('checked', false);
        });
    });

    $(function () {
        $("#checkSpring").click(function () {
            if ($(this).is(":checked")) {
                $(".spring").show();
                $(".colSpring").hide();
            } else {
                $(".spring").hide();
                $(".colSpring").show();
            }
        });
    });

    $(function () {
        $("#chkAngular").click(function () {
            if ($(this).is(":checked")) {
                $(".angularJS").show();
                $(".colAngular").hide();
            } else {
                $(".angularJS").hide();
                $(".colAngular").show();
            }
        });
    });

    $(function () {
        $("#checkHtml").click(function () {
            if ($(this).is(":checked")) {
                $(".htmlcss").show();
                $(".colHtml").hide();
            } else {
                $(".htmlcss").hide();
                $(".colHtml").show();
            }
        });
    });

    $(function () {
        $("#chkPresta").click(function () {
            if ($(this).is(":checked")) {
                $("#Presta").show();
            } else {
                $("#Presta").hide();
            }
        });
    });

    $(function () {
        $("#chkCollab").click(function () {
            if ($(this).is(":checked")) {
                $(".collabs").show();
            } else {
                $(".collabs").hide();
            }
        });
    });

    $(function () {
        $("#Service-1").click(function () {
            if ($(this).is(":checked")) {
                $(".service-1").show();
            } else {
                $(".service-1").hide();
            }
        });
    });

    $(function () {
        $("#Service-2").click(function () {
            if ($(this).is(":checked")) {
                $(".service-2").show();
            } else {
                $(".service-2").hide();
            }
        });
    });

    $(function () {
        $("#Service-Studio").click(function () {
            if ($(this).is(":checked")) {
                $(".service-studio").show();
            } else {
                $(".service-studio").hide();
            }
        });
    });

    $(function () {
        $("#Archi").click(function () {
            if ($(this).is(":checked")) {
                $(".architecte").show();
            } else {
                $(".architecte").hide();
            }
        });
    });

    $(function () {
        $("#Dev").click(function () {
            if ($(this).is(":checked")) {
                $(".developpeur").show();
            } else {
                $(".developpeur").hide();
            }
        });
    });

    $(function () {
        $("#CDP").click(function () {
            if ($(this).is(":checked")) {
                $("#chef-de-projet").show();
            } else {
                $("#chef-de-projet").hide();
            }
        });
    });