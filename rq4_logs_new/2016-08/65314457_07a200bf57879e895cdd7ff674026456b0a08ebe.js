$.fn.prettyBreak = function () {

    return this.each(function () {

        var element = $(this);

        element.wrapInner("<span style='white-space: nowrap'>");
        const textWidth = element.find("span").width();
        const lineHeight = element.find("span").height();
        element.find("span").removeAttr("style");

        const elementText = $.trim(element.text());

        const splitText = function () {

            const parentWidth = element.parent().width();
            const textHeight = element.find("span").height();

            if (textWidth > parentWidth && textHeight < lineHeight * 3) {

                let middle = Math.floor(elementText.length / 2);
                const before = elementText.lastIndexOf(" ", middle);
                const after = elementText.indexOf(" ", middle + 1);

                if (middle - before < after - middle) {
                    middle = before;
                } else {
                    middle = after;
                }

                const s1 = elementText.substr(0, middle);
                const s2 = elementText.substr(middle + 1);

                element.find("span").html(s1 + "<br>" + s2);

            } else {
                element.find("span").html(elementText);
            }
        };

        splitText();

        $(window).resize(function () {

            splitText();

        });
    });
}