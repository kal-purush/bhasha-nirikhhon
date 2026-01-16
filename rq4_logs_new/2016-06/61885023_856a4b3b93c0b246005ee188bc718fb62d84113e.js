jQuery(document).ready(function($) {

    /* efectos jquery smoove*/

    $('.about').smoove({offset:'40%'});

    $('.lastest').smoove({offset:'40%'});

    $('.projects').smoove({offset:'40%'});

    $('.experience').smoove({offset:'40%'});

    $('.github').smoove({rotate3d:'1,1,1,90deg'});

    $('.info').smoove({offset:'40%'});

    $('.skills').smoove({offset:'40%'});

    $('.testimonials').smoove({offset:'40%'});

    $('.education').smoove({offset:'40%'});

    $('.languages').smoove({offset:'40%'});

    $('.blog').smoove({offset:'40%'});

    $('.music').smoove({offset:'40%'});

    $('.conferences').smoove({offset:'40%'});

    $('.credits').smoove({offset:'40%'});



    /*======= Skillset *=======*/

    $('.level-bar-inner').css('width', '0');

    $(window).on('load', function() {

        $('.level-bar-inner').each(function() {

            var itemWidth = $(this).data('level');

            $(this).animate({
                width: itemWidth
            }, 800);
        });
    });


    $(".section").mouseenter(function(){
        $(this).css("background-color", "rgba(255,255,255,1)");

    });

    $(".section").mouseleave(function(){
        $(this).css("background-color", "rgba(255,255,255,.0)");

    });


    /* Bootstrap Tooltip for Skillset */
    $('.level-label').tooltip();

    /* jQuery RSS - https://github.com/sdepold/jquery-rss */
    $("#rss-feeds").rss(

        //Change this to your own rss feeds
        "http://feeds.feedburner.com/TechCrunch/startups",

        {
        // how many entries do you want?
        // default: 4
        // valid values: any integer
        limit: 3,

        // the effect, which is used to let the entries appear
        // default: 'show'
        // valid values: 'show', 'slide', 'slideFast', 'slideSynced', 'slideFastSynced'
        effect: 'slideFastSynced',

        // outer template for the html transformation
        // default: "<ul>{entries}</ul>"
        // valid values: any string
        layoutTemplate: "<div class='item'>{entries}</div>",

        // inner template for each entry
        // default: '<li><a href="{url}">[{author}@{date}] {title}</a><br/>{shortBodyPlain}</li>'
        // valid values: any string
        entryTemplate: '<h3 class="title"><a href="{url}" target="_blank">{title}</a></h3><div><p>{shortBodyPlain}</p><a class="more-link" href="{url}" target="_blank"><i class="fa fa-external-link"></i>Read more</a></div>'

        }
    );
    
    /* Github Activity Feed - https://github.com/caseyscarborough/github-activity */
    GitHubActivity.feed({ username: "acenaga", selector: "#ghfeed" });


});
