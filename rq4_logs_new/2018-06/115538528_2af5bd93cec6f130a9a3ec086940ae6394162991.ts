import { cssRule } from '../../styles/index';

cssRule('.carousel', {
    "background": "#f8f9fa",
    "display": "block",
    "overflow": "hidden",
    "-webkit-overflow-scrolling": "touch",
    "position": "relative",
    "width": "100%",
    "zIndex": 1
});
cssRule('.carousel .carousel-container', {
    "height": "100%",
    "left": 0,
    "position": "relative"
});
cssRule('.carousel .carousel-container::before', {
    "content": "\"\"",
    "display": "block",
    "paddingBottom": "56.25%"
});
cssRule('.carousel .carousel-container .carousel-item', {
    "animation": "carousel-slideout 1s ease-in-out 1",
    "height": "100%",
    "left": 0,
    "margin": 0,
    "opacity": 0,
    "position": "absolute",
    "top": 0,
    "width": "100%"
});
cssRule('.carousel .carousel-container .carousel-item:hover .item-next,.carousel .carousel-container .carousel-item:hover .item-prev', {
    "opacity": 1
});
cssRule('.carousel .carousel-container .item-next,.carousel .carousel-container .item-prev', {
    "background": "rgba(231,233,237,.25)",
    "borderColor": "rgba(231,233,237,.5)",
    "color": "#e7e9ed",
    "opacity": 0,
    "position": "absolute",
    "top": "50%",
    "transform": "translateY(-50%)",
    "transition": "all .4s ease",
    "zIndex": 100
});
cssRule('.carousel .carousel-container .item-prev', {
    "left": "1rem"
});
cssRule('.carousel .carousel-container .item-next', {
    "right": "1rem"
});
cssRule('.carousel .carousel-locator:nth-of-type(1):checked~.carousel-container .carousel-item:nth-of-type(1),.carousel .carousel-locator:nth-of-type(2):checked~.carousel-container .carousel-item:nth-of-type(2),.carousel .carousel-locator:nth-of-type(3):checked~.carousel-container .carousel-item:nth-of-type(3),.carousel .carousel-locator:nth-of-type(4):checked~.carousel-container .carousel-item:nth-of-type(4)', {
    "animation": "carousel-slidein .75s ease-in-out 1",
    "opacity": 1,
    "zIndex": 100
});
cssRule('.carousel .carousel-locator:nth-of-type(1):checked~.carousel-nav .nav-item:nth-of-type(1),.carousel .carousel-locator:nth-of-type(2):checked~.carousel-nav .nav-item:nth-of-type(2),.carousel .carousel-locator:nth-of-type(3):checked~.carousel-nav .nav-item:nth-of-type(3),.carousel .carousel-locator:nth-of-type(4):checked~.carousel-nav .nav-item:nth-of-type(4)', {
    "color": "#e7e9ed"
});
cssRule('.carousel .carousel-nav', {
    "bottom": ".4rem",
    "display": [
        "flex",
        "-ms-flexbox"
    ],
    "-ms-flex-pack": "center",
    "justifyContent": "center",
    "left": "50%",
    "position": "absolute",
    "transform": "translateX(-50%)",
    "width": "10rem",
    "zIndex": 100
});
cssRule('.carousel .carousel-nav .nav-item', {
    "color": "rgba(231,233,237,.5)",
    "display": "block",
    "-ms-flex": "1 0 auto",
    "flex": "1 0 auto",
    "height": "1.6rem",
    "margin": ".2rem",
    "maxWidth": "2.5rem",
    "position": "relative"
});
cssRule('.carousel .carousel-nav .nav-item::before', {
    "background": "currentColor",
    "content": "\"\"",
    "display": "block",
    "height": ".1rem",
    "position": "absolute",
    "top": ".5rem",
    "width": "100%"
});
cssRule('@keyframes carousel-slidein', {
    "$nest": {
        "0%": {
            "transform": "translateX(100%)"
        },
        "100%": {
            "transform": "translateX(0)"
        }
    }
});
cssRule('@keyframes carousel-slideout', {
    "$nest": {
        "0%": {
            "opacity": 1,
            "transform": "translateX(0)"
        },
        "100%": {
            "opacity": 1,
            "transform": "translateX(-50%)"
        }
    }
});