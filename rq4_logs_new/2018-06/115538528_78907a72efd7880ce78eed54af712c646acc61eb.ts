import { stylesheet } from '@barlus/styles/index';
const { animation } = stylesheet('animations.css');

animation('slide-down', {
    "0%": {
        "opacity": 0,
        "transform": "translateY(-1.6rem)"
    },
    "100%": {
        "opacity": 1,
        "transform": "translateY(0)"
    }
});

animation('loading', {
    "0%": {
        "transform": "rotate(0)"
    },
    "100%": {
        "transform": "rotate(360deg)"
    }
});

animation('carousel-slidein', {
    "0%": {
        "transform": "translateX(100%)"
    },
    "100%": {
        "transform": "translateX(0)"
    }
});

animation('carousel-slideout', {
    "0%": {
        "opacity": 1,
        "transform": "translateX(0)"
    },
    "100%": {
        "opacity": 1,
        "transform": "translateX(-50%)"
    }
});

animation('first-run', {
    "0%": {
        "width": 0
    },
    "25%": {
        "width": "2.4rem"
    },
    "50%": {
        "width": ".8rem"
    },
    "75%": {
        "width": "1.2rem"
    },
    "100%": {
        "width": 0
    }
});

animation('progress-indeterminate', {
    "0%": {
        "backgroundPosition": "200% 0"
    },
    "100%": {
        "backgroundPosition": "-200% 0"
    }
});