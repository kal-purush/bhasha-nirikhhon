$(document).ready(function () {

    var LIST = $('.products');
    var LIST2 = $('.name-of-left-products');
    var LIST3 = $('.name-of-bought-products');
    add();

    function addItem(title) {
        var node = $(".products-container:first").clone();
        var node2 = $(".left-item-label:first").clone();
        var node3 = $(".bought-item-label:first").clone();
        node.css("display", "block");
        node.find(".name-of-item").val(title);
        node2.css("display", "inline-block");
        node2.find(".name-of-left-item .title").text(title);
        node3.css("display", "inline-block");
        node3.find(".name-of-bought-item .title").text(title);
        plus(node, node2, node3);
        minus(node, node2, node3);
        remove(node, node2, node3);
        buy(node, node2, node3);
        unBuy(node, node2, node3);
        editText(node, node2, node3);
        LIST.append(node);
        LIST2.append(node2);
        LIST3.append(node3);
        node3.hide();

    }

    function plus(node, node2, node3) {
        node.find(".green-plus-button").click(function () {
            node.find(".red-minus-icon").prop("disabled", false);
            var quantity = parseInt(node.find(".counter").find("span").text(), "10");
            node.find(".counter").find("span").text(quantity + 1);
            node2.find(".number-of-items").text(quantity + 1);
            node3.find(".number-of-bought-items").text(quantity + 1);
        });
    }

    function minus(node, node2, node3) {
        node.find(".red-minus-icon").click(function () {
            var quantity = parseInt(node.find(".counter").find("span").text(), "10");
            if (quantity > 1) {
                node.find(".red-minus-icon").prop("disabled", false);
                node.find(".red-minus-icon");
                node.find(".counter").find("span").text(quantity - 1);
                node2.find(".number-of-items").text(quantity - 1);
            }
            if (quantity == 2) {
                node.find(".red-minus-icon").prop("disabled", true);
            }
        });
    }

    function remove(node, node2, node3) {
        node.find(".red-delete-button").click(function () {
            node.hide();
            node2.hide();
            node3.hide();
        });
    }


    function add() {
        $(".add-button").click(function () {
            var title = $(".text-area").val();
            addItem(title);
            $(".text-area").val("");
            $(".text-area").focus();
        });
    }

    function buy(node, node2, node3) {
        node.find(".bought-button").click(function () {
            node.fadeOut(250, function () {
                node3.show();
                node2.hide();
                node.find(".name-of-item").css("text-decoration", "line-through");
                node.find(".counter").css("text-decoration", "line-through");
                node.find(".bought-button").css("display", "none");
                node.find(".red-minus-icon").css("visibility", "hidden");
                node.find(".red-delete-button").css("visibility", "hidden");
                node.find(".green-plus-button").css("visibility", "hidden");
                node.find(".unBought-button").css("display", "flex");
                node.fadeIn(250);
                node2.fadeOut(250);
                node3.fadeIn(250);
            })
        })
    }

    function unBuy(node, node2, node3) {
        node.find(".unBought-button").click(function () {
            node.fadeOut(250, function () {
                node3.hide();
                node2.show();
                node.find(".name-of-item").css("text-decoration", "none");
                node.find(".counter").css("text-decoration", "none");
                node.find(".unBought-button").css("display", "none");
                node.find(".bought-button").css("display", "flex");
                node.find(".red-minus-icon").css("visibility", "visible");
                node.find(".red-delete-button").css("visibility", "visible");
                node.find(".green-plus-button").css("visibility", "visible");
                node.fadeIn(250);

            })
        })
    }

    function editText(node, node2, node3) {
        node.find(".name-of-item").focusout(function () {
            var title = node.find(".name-of-item").val();
            node.find(".name-of-item").val(title);
            node.find(".name-of-item").text(title);
            node.find(".name-of-item").show();
            node2.find(".title").text(title);
            node3.find(".title").text(title);
        })
    }

    addItem("Помідори");
    addItem("Печиво");
    addItem("Сир");


})
;