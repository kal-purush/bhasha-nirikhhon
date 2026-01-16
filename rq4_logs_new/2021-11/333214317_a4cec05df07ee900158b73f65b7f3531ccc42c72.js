$('.course_carousel').owlCarousel({
    loop: true,
    margin: 10,
    nav: true,
    dots: false,
    responsive: {
        0: {
            items: 2,
            margin: 5
        },
        650: {
            items: 3
        },
        990: {
            items: 4
        }
    }
})

$('.viewing_carousel').owlCarousel({
    loop: true,
    margin: 10,
    nav: false,
    dots: false,
    responsive: {
        0: {
            items: 2,
            margin: 5
        },
        650: {
            items: 3,
            margin: 5
        },
        770: {
            items: 4,

        },
        1100: {
            items: 6
        }
    }
})

$('.student_carousel').owlCarousel({
    loop: true,
    margin: 15,
    // nav: true,
    dots: false,
    responsive: {
        0: {
            items: 1
        },
        641: {
            items: 2
        },
        1000: {
            items: 3
        }
    }
})