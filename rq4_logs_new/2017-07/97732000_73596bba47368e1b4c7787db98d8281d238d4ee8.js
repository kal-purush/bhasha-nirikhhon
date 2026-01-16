function addRelation() {
    var a = localStorage.getItem("idUser"),
        b = $("#amis").val(),
        c = $("#Montant").val(),
        d = $("#Date").val(),
        e = $("select#typeMonnaie option:selected").val(),
        f = $("input[name=rb]:checked").val();
    $.ajax({
        type: "POST",
        url: "addRelation.php",
        data: {
            idUser: a,
            idAmis: b,
            montant: c,
            idMonnaie: e,
            isLend: f,
            Date: d
        },
        success: function() {
            loadinfo(), closeNav(), $("#amis").val(""), $("#Montant").val(""), $("#Date").val(""), $("input[name=rb]:checked").attr("checked", !1)
        }
    })
}

function openNav() {
    document.getElementById("mySidenav").style.height = "100%", $.ajax({
        type: "POST",
        url: "getMonnaie.php",
        success: function(a) {
            if ("" != a) {
                var b = jQuery.parseJSON(a);
                $("#typeMonnaie").replaceWith('<select name="monnaie" id="typeMonnaie"><option value="Money" disabled selected=selected>Money</option></select>'), $.each(b, function(a, b) {
                    $("#typeMonnaie").append('<option value="' + b.id + '">' + b.TypeMonnaie + "</option>")
                })
            }
        }
    })
}
function openNav2() {
    document.getElementById("mySidenav2").style.height = "100%"
}
function closeNav2() {
    document.getElementById("mySidenav2").style.height = "0"
}

function closeNav() {
    document.getElementById("mySidenav").style.height = "0"
}

function loadinfo() {
    var a = localStorage.getItem("idUser");
    $.ajax({
        type: "POST",
        url: "getUser.php",
        data: {
            idUser: a
        },
        success: function(a) {
            if ("" != a) {
                var b, c, d = "#03a9f4",
                    e = jQuery.parseJSON(a);
                $("#detailUser").replaceWith("<div class=list-block id=detailUser></div>"), $("#detailUser").append('<ul class="addUser"></ul>'), $.each(e, function(a, e) {
                    0 == e.isLend && (c = "#4cd964", d = "#03a9f4", b = '<a href="#" class="mark swipeout-overswipe" style="background: #ff9500;"onclick="returned(' + e.id + ');">Rendu</a>'), 1 == e.isLend && (d = "#03a9f4", c = "#ff2d55", b = '<a href="#" class="mark swipeout-overswipe" style="background: #ff9500;"onclick="returned(' + e.id + ');">Rendu</a>'), 1 == e.rendu && (d = "#888", b = ""), $(".addUser").append('<li class="swipeout"> <div class="swipeout-content"> <a href="#" class="item-content item-link"> <div class="item-inner"> <div class="item-title-row"> <div class="item-title" style="color:' + d + '">&nbsp;' + e.idAmis + '</div><div class="item-after">The  ' + e.Date + '</div></div><div class="item-subtitle"  style="color:' + c + '"> ' + e.Montant + " " + e.TypeMonnaie + '</div></div></a> </div><div class="swipeout-actions-right"> <a href="#" class="swipeout" style="background: #ff3b30;" onclick="deleted(' + e.id + ');">Supprimer</a>' + b + "</div></li>")
                })
            }
        }
    })
}

function deleted(a) {
    $.ajax({
        type: "POST",
        url: "deleteRelation.php",
        data: {
            idRelation: a
        },
        success: function(a) {
            $("#detailUser").replaceWith("<div class=list-block id=detailUser></div>"), loadinfo()
        }
    })
}

function returned(a) {
    $.ajax({
        type: "POST",
        url: "returnCash.php",
        data: {
            idRelation: a
        },
        success: function(a) {
            $("#detailUser").replaceWith("<div class=list-block id=detailUser></div>"), loadinfo()
        }
    })
}
$(".input").on("focus", "input[type=number]", function(a) {
    $(this).on("mousewheel.disableScroll", function(a) {
        a.preventDefault()
    })
}), $(".input").on("blur", "input[type=number]", function(a) {
    $(this).off("mousewheel.disableScroll")
}), jQuery(document).ready(function() {
    function a(a, b) {
        $(".ripple").remove();
        var c = a.offset().top,
            d = a.offset().left,
            e = b.pageX - d,
            f = b.pageY - c,
            g = $("<div class='ripple'></div>");
        g.css({
            top: f,
            left: e
        }), a.append(g)
    }

    function b1(a) {
        $(".alertS").remove(), $(".alertE").remove(), $(".error2").append('<div class="alertE"><strong>Error!</strong>' + a + "</div>")
    }
    function b(a) {
        $(".alertS").remove(), $(".alertE").remove(), $(".error").append('<div class="alertE"><strong>Error!</strong>' + a + "</div>")
    }
    var c = !1,
        d = 1100,
        e = 400,
        f = $(".login"),
        g = $(".app");
    if (null != localStorage.getItem("idUser")) {
        var h = this;
        g.show(), g.css("top"), g.addClass("active"), f.hide(), f.addClass("inactive"), c = !1, $(h).removeClass("success processing")
    }
    $(".burger, .overlay").click(function() {
        $("main").toggleClass("open"), $(".burger").toggleClass("open"), $(".overlay").fadeToggle()
    }), $("#login").click(function() {
        var h = $("#username").val(),
            i = $("#password").val();
        "" != h && "" != i ? $.ajax({
            type: "POST",
            url: "login.php",
            data: {
                username: h,
                password: i
            },
            success: function(h) {
                if ("" != h) {
                    $(document).on("click", "#login", function(b) {
                        if (!c) {
                            c = !0;
                            var h = this;
                            a($(h), b), $(h).addClass("processing"), setTimeout(function() {
                                $(h).addClass("success"), setTimeout(function() {
                                    g.show(), g.css("top"), g.addClass("active")
                                }, e - 70), setTimeout(function() {
                                    f.hide(), f.addClass("inactive"), c = !1, $(h).removeClass("success processing")
                                }, e)
                            }, d)
                        }
                    });
                    var i = JSON.parse(h);
                    localStorage.setItem("idUser", i.id), loadinfo()
                } else b("Invalid login")
            }
        }) : b("Please indicate all fields")
    }), $("#goToRegister").click(function() {
        document.location.href = "register.html"
    }), $("#goToLogin").click(function() {
        document.location.href = "index.html"
    }), $("#register").click(function() {
        var a = $("#username1").val(),
            c = $("#password1").val(),
            d = $("#passwordVerif").val(),
            e = $("#email1").val();
        "" != a && "" != c && "" != e ? c === d ? $.ajax({
            type: "POST",
            url: "register.php",
            data: {
                username: a,
                password: c,
                email: e
            },
            success: function(c) {
                1 == c ? (localStorage.setItem("idUser", a), document.location.href = "index.html") : b1("There is already a user with this pseudo")
            }
        }) : b1("Passwords don't match") : b1("Please indicate all fields")
    })
});