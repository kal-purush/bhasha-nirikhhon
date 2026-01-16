import { useEffect, RefObject } from 'react';
import { gsap } from 'gsap';
import scrollTrigger from 'gsap/dist/ScrollTrigger';

gsap.registerPlugin(scrollTrigger);

interface IParams {
  children?: boolean;
}

type Element = HTMLElement | HTMLCollection | never[] | null;

const useAnimVisible = (
  ref: RefObject<HTMLElement>,
  from: gsap.TweenVars,
  to: gsap.TweenVars,
  params?: IParams,
) => {
  useEffect(() => {
    let el: Element = ref.current;

    if (params?.children) {
      el = ref.current?.children || [];
    }

    const tl = gsap.timeline({
      defaults: { ease: 'power1.inOut' },
      scrollTrigger: ref.current,
      toggleActions: 'play none none none',
    });

    tl.fromTo(el, from, to);

    return () => {
      tl.kill();
    };
  }, []);
};

export default useAnimVisible;