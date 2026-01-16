import { gsap } from 'gsap';

import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { Observer } from 'gsap/Observer';

import 'splitting/dist/splitting.css';
import 'splitting/dist/splitting-cells.css';
import Splitting from 'splitting';

Splitting({ by: 'words' });
gsap.registerPlugin(ScrollTrigger, Observer);

gsap.ticker.lagSmoothing(0);

let sections = document.querySelectorAll('section'),
    outerWrappers = gsap.utils.toArray('.outer'),
    innerWrappers = gsap.utils.toArray('.inner'),
    currentIndex = -1,
    animating;

if (outerWrappers && innerWrappers) {
    gsap.set(outerWrappers, { yPercent: 100 });
    gsap.set(innerWrappers, { yPercent: -100 });
}

function gotoSection(index, direction) {
    console.log(index, direction);
    if (index < 0 || index >= sections.length || !outerWrappers[index] || !innerWrappers[index]) {
        return;
    }
    animating = true;
    let fromTop = direction === -1,
        dFactor = fromTop ? -1 : 1,
        tl = gsap.timeline({
            defaults: { duration: 1.25, ease: 'power1.inOut' },
            onComplete: () => (animating = false),
        });
    if (currentIndex >= 0) {
        gsap.set(sections[currentIndex], { zIndex: 0 });
        tl.set(sections[currentIndex], { autoAlpha: 0 });
    }
    // gsap.set(sections[index], { autoAlpha: 1, zIndex: 1 });

    tl.fromTo(
        [outerWrappers[index], innerWrappers[index]],
        {
            yPercent: (i) => (i ? -100 * dFactor : 100 * dFactor),
        },
        {
            yPercent: 0,
        },
        0
    );
    // Image Animations
    // .fromTo(images[index], { yPercent: 1 * dFactor }, { yPercent: 0, ease: 'bezier-curve(0.65, 0, 0.35, 1)' }, 0);

    currentIndex = index;
}

Observer.create({
    type: 'wheel,touch',
    wheelSpeed: -1,

    onDown: () => {
        !animating ? gotoSection(currentIndex - 1, -1) : null;
    },
    onUp: () => {
        !animating && currentIndex !== currentIndex.length ? gotoSection(currentIndex + 1, 1) : null;
    },
    tolerance: 1,
    // preventDefault: true,
    // this is needed to prevent the default behavior of the refreshing on mobile phones when scrolling up but it's causing the scroll behavour to be funny
});

gotoSection(0, 1);