const callback = function(entries) {
    entries.forEach(entry => {
      animateCSS(entry.target, 'fadeInUp');
    });
  };
  
const observer = new IntersectionObserver(callback);

const targets = document.querySelectorAll(".show-on-scroll");
targets.forEach(function(target) {
    observer.observe(target);
});

const animateCSS = (element, animation, prefix = 'animate__') =>
// We create a Promise and return it
    new Promise((resolve, reject) => {
        const animationName = `${prefix}${animation}`;
        //const node = document.querySelector(element);
        const node = element;
        node.classList.remove('show-on-scroll')
        node.classList.add(`${prefix}animated`, animationName);

        // When the animation ends, we clean the classes and resolve the Promise
        function handleAnimationEnd() {
            //node.classList.remove(`${prefix}animated`, animationName);
            node.removeEventListener('animationend', handleAnimationEnd);

            resolve('Animation ended');
        }

        node.addEventListener('animationend', handleAnimationEnd);
    });