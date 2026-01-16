var pressedKeys = {
    shift: false,
};

$(document).ready(function() {
    var $svg = $('#svg > svg:first');
    var svgHeight = $svg.attr('height');
    var $searchResultContainer = $svg.find('#search-results');
    var $baseChainText = $svg.find('g.chain:first').find('text:first');
    var $clickedGroup;

    $svg.find('g.chain')
        .mouseover(function(e) {
            var $rect = $(this);
            $('#hovered-sub').text($rect.find('title:first').text());
        })
        .mouseout(function(e) {
            $('.extra-hovered').removeClass('extra-hovered');
            if($('#hovered-sub').text() === $(this).find('title:first').text()) {
                $('#hovered-sub').text('');
            }    
        })
        .click(function(e) {
            var $group = $(this);
            $clickedGroup = $group;
            var $rect = $group.find('rect:first');
            var $clickedText = $group.find('text:first');
            var svgWidth = $svg.attr('width');
            var fontWidth = $svg.data('font-width');

            var subName = $group.data('name');

            // if the bottom bar is clicked -> nothing to fetch
            if(subName) {
                $.get($('#source-list').data('sub-source-url') + subName, function(data) {
                    $('#source-list').html(data);
                }).fail(function() {
                    $('#source-list').html('No source to show.');
                });
            }

            $('.zoomed').removeClass('zoomed');
            $('.zoom-ancestor').removeClass('zoom-ancestor');
            $('.zoom-descendant').removeClass('zoom-descendant');
            $('.zoom-unwanted').removeClass('zoom-unwanted');
            $('.zoom-too-thin').removeClass('zoom-too-thin');

            var oldOrigin = parseFloat($rect.data('orig-x') || $rect.attr('x'));
            var oldWidth = parseFloat($rect.data('orig-width') || $rect.attr('width'));
            var oldY = parseFloat($rect.attr('y'));

            changeChainAppearance($group, 0, svgWidth, fontWidth);
            $group.addClass('zoomed');

            $svg.find('g.chain:not(.zoomed)').each(function() {
                var $foundGroup = $(this);

                var $foundRect = $foundGroup.find('rect:first');
                var $foundText = $foundGroup.find('text:first');
                var $foundBorderRight = $foundGroup.find('line.border-right:first');

                var foundX = parseFloat($foundRect.data('orig-x') || $foundRect.attr('x'));
                var foundWidth = parseFloat($foundRect.data('orig-width') || $foundRect.attr('width'));
                var foundY = parseFloat($foundRect.attr('y'));

                var isAbove = foundY < oldY;
                var isBelow = foundY > oldY;

                var isDescendant = isAbove
                              && foundX >= oldOrigin                         // starts at or right of the start of the clicked area
                              && foundX < oldOrigin + oldWidth;              // starts left of the end of the clicked area
                var isAncestor = isBelow
                              && foundX < oldOrigin + oldWidth               // starts left of the end of the clicked area
                              && foundX + foundWidth > oldOrigin;            // ends right of the start of the clicked area

                if(!isAncestor && !isDescendant) {
                    $foundGroup.addClass('zoom-unwanted');
                    return;
                }
                else {

                    if(isAncestor) {
                        changeChainAppearance($foundGroup, 0, svgWidth, fontWidth);
                        $foundGroup.addClass('zoom-ancestor');
                    }
                    else if(isDescendant) {
                        var newX = floatRound(svgWidth * ((foundX - oldOrigin) / oldWidth), 3);

                        var newWidth = floatRound(svgWidth * (foundWidth / oldWidth), 3);
                        changeChainAppearance($foundGroup, newX, newWidth, fontWidth);
                        $foundGroup.addClass('zoom-descendant');
                    }
                }
            });
            updateBaseChainText($group);
            $('#search-form').submit();
        });

    $('#search-reset').click(function(e) {
        $('#search-form').find('input').val('').end().submit();
    });
    $('#search-form').submit(function(e) {
        e.preventDefault();
        $searchResultContainer.empty();
        var searchString = $('#search').val();
        var searchRegex;
        if(searchString === '') {
            updateBaseChainText($clickedGroup);
            return;
        }
        var negativeSearch = searchString.substring(0, 1) === '!' ? 1 : 0;
        if(negativeSearch) {
            searchString = searchString.substring(1);
        }

        try {
            searchRegex = new RegExp(searchString);
        }
        catch(e) {
            console.log("'" + searchString + "' is not a regex");
        }
        if(searchRegex) {
            var count = 0;
            var percent = 0;
            var checkCount = 0;
            var microSeconds = 0;

            $svg.find('g.chain:not(.zoom-unwanted)').each(function() {
                var $group = $(this);
                var $title = $group.find('title:first');
                var $rect = $group.find('rect:first');

                // possibly invert search result
                var searchMatches = $group.data('name').match(searchRegex);

                if(searchMatches) {
                    $group.addClass('search-result');
console.log($group, $group.find('rect:first').attr('x'), $group.find('rect:first').attr('width'), $group.attr('class'));
                    var matchX = parseFloat($rect.attr('x'));
                    var matchWidth = parseFloat($rect.attr('width'));

                    var hasOverlap = 0;
                    // Find already existing search result highlighters so that
                    // we don't create unnecessary <rect>s and can
                    // calculate correct percentage-
                    $searchResultContainer.find('rect.search-highlighter, rect.search-highlighter-unwanted').each(function() {

                        var $highlighted = $(this);
 
                        if(    parseFloat($highlighted.attr('x')) <= matchX
                            && parseFloat($highlighted.attr('x')) + parseFloat($highlighted.attr('width')) >= matchX + matchWidth) {
                            hasOverlap++;
                            return false;
                        }
                    });

                    count++;
                    var percentText = $group.data('percent');
                    if(!hasOverlap && undefined !== percentText) {
                        percent += parseFloat(percentText);
                        microSeconds += cleanNumber($group.data('ms'));
                    }
                    if(!hasOverlap) {
                        var highligherClass = negativeSearch ? 'search-highlighter-unwanted' : 'search-highlighter';
                        var x = $rect.attr('width') < 1 ? $rect.attr('x') - 0.5 : $rect.attr('x');
                        var width = $rect.attr('width') < 1 ? 1 : $rect.attr('width');

                        addSearchHighlighter({
                            x: x,
                            width: width,
                            class: highligherClass,
                        });
                    }
                }
            });

            // negative search: mark all un-highlighted areas
            if(negativeSearch) {
                var previousX = $svg.attr('width');
                var allUnwanted = [];

                $searchResultContainer.find('.search-highlighter-unwanted').each(function() {
                    var $highlighter = $(this);
                    var highlightX = parseFloat($highlighter.attr('x'));
                    var highlightWidth = parseFloat($highlighter.attr('width'));
                    var highlightEnd = highlightX + highlightWidth;
                    allUnwanted.push({ x: highlightX, end: highlightEnd});

                    if(highlightEnd < previousX) {
                        var negativeHighlighter = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
                        var distanceBetween = previousX - highlightEnd;

                        var x = distanceBetween < 1 ? distanceBetween - 0.5 : distanceBetween;
                        var width = distanceBetween < 1 ? 1 : distanceBetween;

                        addSearchHighlighter({
                            x: highlightEnd,
                            width: width,
                            class: 'search-highlighter',
                        });
                    }
                    previousX = highlightX;
                });
                if(previousX > 0) {

                    addSearchHighlighter({
                        x: 0,
                        width: previousX,
                        class: 'search-highlighter',
                    });
                }
            }

            if(negativeSearch) {
                var shownMs = parseFloat(cleanNumber($clickedGroup ? $clickedGroup.data('ms') : $baseChainText.parent().data('ms')));
                updateBaseChainText($clickedGroup, shownMs - microSeconds, floatRound(100 - percent, 2), searchString, negativeSearch);
            }
            else {
                updateBaseChainText($clickedGroup, microSeconds, percent, searchString, negativeSearch);
            }
        }
    });

    $(document).keydown(function(e) {
            if(e.shiftKey) {
                pressedKeys['shift'] = true;
            }
        })
        .keyup(function(e) {
            if(!e.shiftKey) {
                pressedKeys['shift'] = false;
            }
        });

    function updateBaseChainText($infoGroup, microSeconds, percent, searchString, isNegativeSearch) {
        if(!$infoGroup) {
            console.log('default group');
            $infoGroup = $baseChainText.parent();
        }
        var msAsNum = parseFloat(cleanNumber($infoGroup.data('ms')));
        var newText = 'showing: ' + $infoGroup.data('ms')+'ms (' + $infoGroup.data('percent') + '%)';
        if(undefined !== microSeconds) {
            var searchPercentOfShown = floatRound(100 * cleanNumber(microSeconds) / msAsNum, 2);
            microSeconds = formatNumber(microSeconds);
            var searchText = searchString ? '/' + searchString + '/' : '';
            searchText = isNegativeSearch ? '!' + searchText : searchText;
            newText = newText + ' of which ' + microSeconds + 'ms (' + floatRound(searchPercentOfShown, 2) + '%) matches ' + searchText;
        }
        $baseChainText.text(newText);
    }
    function addSearchHighlighter(settings) {
        var highlighter = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        highlighter.setAttribute('x', floatRound(settings.x, 4));
        highlighter.setAttribute('y', 0);
        highlighter.setAttribute('width', floatRound(settings.width, 4));
        highlighter.setAttribute('height', svgHeight - 20);
        highlighter.setAttribute('class', settings.class);
        $searchResultContainer.get(0).appendChild(highlighter);
    }
});
function formatNumber(number) {
    return ('' + number).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}
function cleanNumber(number) {
    try {
        return parseFloat(number.replace(/,/g, ''));
    }
    catch(e) {
        return number;
    }
}
function longestPossibleText(text, width, fontWidth) {

    var maxTextLength = Math.floor(width / fontWidth);
    return text.length > maxTextLength ? maxTextLength >= 3 ? text.substr(0, maxTextLength - 1) + '…'
                                       :                      ''
         :                               text
         ;
}
function floatRound(number, precision) {
    return Number(Math.round(number + 'e' + precision) + 'e-' + precision);
}

function changeChainAppearance($group, newX, newWidth, fontWidth) {
    var $rect = $group.find('rect:first');
    var $text = $group.find('text:first');
    var $borderRight = $group.find('line.border-right:first');

    if(!$rect.data('orig-x')) {
        $rect.attr('data-orig-x', $rect.attr('x'));
        $rect.attr('data-orig-width', $rect.attr('width'));
    }

    $rect.attr('x', newX)
         .attr('width', newWidth);
    $borderRight.attr('x1', newX + newWidth)
                .attr('x2', newX + newWidth);

    $text.attr('x', newX + 2.5);
    $text.text(longestPossibleText($group.data('name'), $rect.attr('width'), fontWidth));

    if($rect.attr('width') < 0.1) { $group.addClass('zoom-too-thin'); }

    $group.addClass('zoom-descendant');
}