    function featuredbwidget(a) {
        (function(e) {
            var h = {
                listURL: "",
                featuredNum: 3,
                featuredID: "",
                feathumbSize: 350,
                interval: 0,
                autoplay: false,
                loadingFeatured: "nextfeatured",
                pBlank: "https://3.bp.blogspot.com/-EOu4Rrgcryo/V0m8dV7MU1I/AAAAAAAALlg/4h5vQaHpQiMdkvtUdDbu0LtjJRvgPERYwCLcB/s500/no-image.png",
                byMonth: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
                listbyLabel: false
            };
            h = e.extend({}, h, a);
            var g = e(h.featuredID);
            var d = h.featuredNum * 200;
            g.html('<div class="featslider"><ul class="featured-widget-list"></ul><div class="feat-buttons"><a href="#" class="feat-prev">Prev</a><a href="#" class="feat-next">Next</a></div></div>');
            var f = function(w) {
                var q, k, m, u, x, p, t, v, r, l = "",
                    s = w.feed.entry;
                for (var o = 0; o < s.length; o++) {
                    for (var n = 0; n < s[o].link.length; n++) {
                        if (s[o].link[n].rel == "alternate") {
                            q = s[o].link[n].href;
                            break
                        }
                    }
                    if ("media$thumbnail" in s[o]) {
                        u = s[o].media$thumbnail.url.replace(/\/s[0-9]+\-c/g, "/s" + h.feathumbSize + "-c")
                    } else {
                        u = h.pBlank.replace(/\/s[0-9]+(\-c|\/)/, "/s" + h.feathumbSize + "$1")
                    }
                    k = s[o].title.$t;
                    r = s[o].published.$t.substring(0, 10);
                    m = s[o].author[0].name.$t;
                    x = r.substring(0, 4);
                    p = r.substring(5, 7);
                    t = r.substring(8, 10);
                    v = h.byMonth[parseInt(p, 10) - 1];
                    l += '<li><a href="' + q + '"><div class="featuredbg"></div><img class="featuredthumb" src="' + u + '"/><div class="feature-info"><h5>' + k + '</h5></a><div class="featured-meta"><span class="fdate"><span class="fday">' + t + '</span> <span class="fmonth">' + v + '</span> <span class="fyear">' + x + '</span></span> - <span class="fauthor">' + m + "</span></div></div></li>"
                }
                e("ul", g).append(l).addClass(h.loadingFeatured)
            };
            var c = function() {
                e(h.featuredID + " .feat-next").click()
            };
            var b = function() {
                e.get((h.listURL === "" ? window.location.protocol + "//" + window.location.host : h.listURL) + "/feeds/posts/summary" + (h.listbyLabel === false ? "" : "/-/" + h.listbyLabel) + "?max-results=" + h.featuredNum + "&orderby=published&alt=json-in-script", f, "jsonp");
                setTimeout(function() {
                    e(h.featuredID + " .feat-prev").click(function() {
                        e(h.featuredID + " .featslider li:first").before(e(h.featuredID + " .featslider li:last"));
                        return false
                    });
                    e(h.featuredID + " .feat-next").click(function() {
                        e(h.featuredID + " .featslider li:last").after(e(h.featuredID + " .featslider li:first"));
                        return false
                    });
                    if (h.autoplay) {
                        var i = h.interval;
                        var j = setInterval(c, i);
                        e(h.featuredID + " .featslider li:first").before(e(h.featuredID + " .featslider li:last"));
                        e(h.featuredID + " .featslider").hover(function() {
                            clearInterval(j)
                        }, function() {
                            j = setInterval(c, i)
                        })
                    }
                    e("ul", g).removeClass(h.loadingFeatured)
                }, d)
            };
            e(document).ready(b)
        })(jQuery)
    };