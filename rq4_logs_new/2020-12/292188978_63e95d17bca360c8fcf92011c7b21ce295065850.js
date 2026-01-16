$(document).ready(function () {
    //we write everything here

    $('.add-cart').click(function () {
        $('.count').html(function (i, val) {
            return val * 1 + 1
        });
    });

    $(document).ready(function () {

        //hides dropdown content
        $(".size_chart").hide();

        //unhides first option content
        $("#option1").show();

        //listen to dropdown for change
        $("#size_select").change(function () {
            //rehide content on change
            $('.size_chart').hide();
            //unhides current item
            $('#' + $(this).val()).show();
        });

    });



    $('#carouselExampleCaptions').carousel({
        interval: 3000,
    });

    // $('#homePageCarousel').on('slid.bs.carousel', function () {
    //     console.log(this);
    //     $('.logo').each(function(){

    //         new CircleType(this).dir(-1).radius(384);
    //     })
    // })


});