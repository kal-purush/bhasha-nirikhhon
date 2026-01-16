AOS.init();
// Product slider End
$('.slider').slick({
    arrows: true,
    infinite: true,
    slidesToShow: 4,
    accessibility: true,
    autoplay: true,
    autoplaySpeed: 3000,
    prevArrow: "<span class='arrow prev'><i class='fas fa-angle-left'></i></span>",
    nextArrow: "<span class='arrow next'><i class='fas fa-angle-right'></i></span>",
    customPaging: function(slider, i) {
        /* ADDING CUSTOM PAGING
        Example
        return  return '<li>Something you want to insert</li>';
*/
    },
    responsive: [{
            breakpoint: 1024,
            settings: {
                slidesToShow: 3,
                slidesToScroll: 3,
                infinite: true,
                dots: false,
                arrows: false
            }
        },
        {
            breakpoint: 600,
            settings: {
                slidesToShow: 2,
                slidesToScroll: 2,
                arrows: false,
                autoplay: true,
                autoplaySpeed: 3000
            }
        },
        {
            breakpoint: 480,
            settings: {
                slidesToShow: 1,
                slidesToScroll: 1,
                arrows: false,
                autoplay: true,
                autoplaySpeed: 3000
            }
        }
        // You can unslick at a given breakpoint now by adding:
        // settings: "unslick"
        // instead of a settings object
    ]
});
// Product Slider End

// Counter Start
$('.counting').each(function() {
    var $this = $(this),
        countTo = $this.attr('data-count');

    $({ countNum: $this.text() }).animate({
        countNum: countTo
    }, {
        duration: 4000,
        easing: 'linear',
        step: function() {
            $this.text(Math.floor(this.countNum));
        },
        complete: function() {
            $this.text(this.countNum);
            //alert('finished');
        }
    });
});
// Counter End
// See more start
$(".readmore-link").click(function(e) {
    var isExpanded = $(e.target).hasClass("expand");
    $(".contentNews.expand").removeClass("expand");
    $(".readmore-link.expand").removeClass("expand");
    // if target wasn't expand, then expand it
    if (!isExpanded) {
        $(e.target).parent(".contentNews").addClass("expand");
        $(e.target).addClass("expand");
    }
});

// $(document).ready(function() {
//     $(".toggle1").click(function() {
//         var elem = $(".toggle1").text();
//         if (elem == "￬") {
//             $(".toggle1").text("￪");
//             $(".text1").slideDown();
//         } else {
//             $(".toggle1").text("￬");
//             $(".text1").slideUp();
//         }
//     });
// });
// $(document).ready(function() {
//     $(".toggle2").click(function() {
//         var elem = $(".toggle2").text();
//         if (elem == "￬") {
//             $(".toggle2").text("￪");
//             $(".text2").slideDown();
//         } else {
//             $(".toggle2").text("￬");
//             $(".text2").slideUp();
//         }
//     });
// });
//   See more end

// Loading Start
$(document).ready(function() {
    var loading = $(".loading");
    loading.delay(loading.attr("delay-hide")).slideUp();
});
//   Loading End