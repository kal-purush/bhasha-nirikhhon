// src/plugins/AnimationObserverPlugin.ts
import { App } from 'vue';

const AnimationObserverPlugin = {
    install(app: App) {
        const observerOptions = {
            root: null,
            rootMargin: '0px',
            threshold: 0.1
        };

        const observerCallback = (entries: IntersectionObserverEntry[]) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    entry.target.classList.add('animate');
                }
            });
        };

        const observer = new IntersectionObserver(observerCallback, observerOptions);

        app.directive('observe-animation', {
            mounted(el) {
                if (el.classList.contains('animation')) {
                    observer.observe(el);
                }
            }
        });
    }
};

export default AnimationObserverPlugin;