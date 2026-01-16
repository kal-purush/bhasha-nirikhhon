$(document).ready(function() {
    $("#rotate").click(function() {
        $(".tile").attr("class", function(ind, val) {
            let split_arr = val.split(" ");
            let coords = split_arr[2];
            let x = parseInt(coords[1]);
            let y = parseInt(coords[3]);
            let z = parseInt(coords[5]);
            let m = Math.max(x, y, z);
            split_arr[2] = `x${m - y}y${m - z}z${m - x}`;
            return split_arr.join(" ");
        });
    });

    $("#toggle").click(function() {
        if ($("#toggle").text() === "Banners") $("#toggle").text("Relics");
        else if ($("#toggle").text() === "Relics") $("#toggle").text("Colours");
        else $("#toggle").text("Banners");
    });

    $("#clear_banners").click(function() {
        $(".banner").remove();
    });

    $("#clear_relics").click(function() {
        $("img").not(".banner").remove();
    });

    $("#clear_colours").click(function() {
        $(".tile").not(".home").children(".purple, .pink, .green, .blue, .yellow, .red").attr("class","hexagon-inner");
    });

    $("#import").click(function() {
        $("#upload").trigger("click");
    });

    $("#upload").change(function(e) {
        let file = e.target.files[0];
        let reader = new FileReader();
        reader.readAsText(file);
        reader.onload = function() {
            try {
                config = JSON.parse(reader.result);
                for (let i = 0; i < 8; i++) {
                    for (let j = 0; j < 8; j++) {
                        for (let k = 0; k < 8; k++) {
                            if (i === 0 || j === 0 || k === 0) {
                                let tile_id = `x${i}y${j}z${k}`;
                                let colour = config[tile_id]["colour"];
                                let image = config[tile_id]["image"];
                                let inner = $(`.${tile_id}`).children().first();
                                if (colour !== null) inner.attr("class", `hexagon-inner ${colour}`);
                                else inner.attr("class", "hexagon-inner");
                                if (image !== null) inner.html(`<img class="${image}" src="images/${image}.webp">`);
                                else inner.html("");
                            }
                        }
                    }
                }
            } catch {}
        };
    });

    $("#export").click(function() {
        let text = "{";
        for (let i = 0; i < 8; i++) {
            for (let j = 0; j < 8; j++) {
                for (let k = 0; k < 8; k++) {
                    if (i === 0 || j === 0 || k === 0) {
                        let tile_id = `x${i}y${j}z${k}`;
                        let inner = $(`.${tile_id}`).children().first();
                        let colour = inner.attr("class").split(" ")[1] ? `"${inner.attr("class").split(" ")[1]}"` : null;
                        let image = inner.children().first().length ? `"${inner.children().first().attr("class")}"` : null;
                        text += `"${tile_id}":{"colour":${colour},"image":${image}}`;
                        if (i !== 7 || j !== 7) text += ",";
                    }
                }
            }
        }
        text += "}";

        let download = $("<a></a>");
        download.attr("href", "data:text/plain;charset=utf-8," + encodeURIComponent(text));
        download.attr("download", "config.json");
        download.css("display", "none");
        $("body").append(download);
        $("a")[0].click();
        $("a").remove();
    });

    $(".tile").not(".home").click(function() {
        let inner = $(this).children().first();
        let home_colour = $(".x7y0z7").children().first().attr("class").split(" ")[1];
        let colour = inner.attr("class").split(" ")[1] ? inner.attr("class").split(" ")[1] : null;
        let image = inner.children().first().length ? inner.children().first().attr("class") : null;
        
        if ($("#toggle").text() === "Banners") {
            if (image === "banner") inner.html("");
            else if (image === null) inner.html("<img class=\"banner\" src=\"images/banner.webp\">");
        } else if ($("#toggle").text() === "Relics") {
            if (image === null) {
                $("#modal").modal();
                $(".custom-select").attr("id", $(this).attr("class").split(" ")[2]);
            } else if (image !== "banner") {
                inner.html("");
            }
        } else {
            if (colour === home_colour) inner.removeClass(home_colour);
            else inner.attr("class", `hexagon-inner ${home_colour}`);
        }
    });

    $("#save_relics").click(function() {
        let tile_id = $(".custom-select").attr("id");
        let relic = $(".custom-select :selected").text();
        $(`.${tile_id}`).children().first().html(`<img class="${relic}" src="images/${relic}.webp">`);
        $("#modal").modal("hide");
    });
});