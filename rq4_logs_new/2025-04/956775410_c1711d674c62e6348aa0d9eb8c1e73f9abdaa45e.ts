// hooks/useBouncyText.ts
import { useEffect } from "react";
import { gsap } from "gsap";
import { TextPlugin } from "gsap/TextPlugin";
import { ScrollTrigger } from "gsap/ScrollTrigger";

gsap.registerPlugin(TextPlugin, ScrollTrigger);

export function useAnimateText({
  containerRef,

  text,
  duration = 2,
  delay = 0.3,
  // ease = "elastic.out(1, 1)",
  ease = "power2.out",
  start = "bottom 80%",
}: {
  containerRef: React.RefObject<HTMLElement | null>;
  text: string;
  duration?: number;
  delay?: number;
  ease?: string;
  start?: string;
}) {
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const ctx = gsap.context(() => {
      const tl = gsap.timeline({
        scrollTrigger: {
          trigger: el,
          start,
          toggleActions: "play none none reset", // 👈 reset so it re-triggers
        },
      });

      // tl.fromTo(
      //   el,
      //   {
      //     text: "",
      //     scale: 0.6,
      //     y: 50,
      //     opacity: 0,
      //   },
      //   {
      //     text,
      //     scale: 1,
      //     y: 0,
      //     opacity: 1,
      //     duration,
      //     ease,
      //     delay,
      //   }  // Step 1: From empty to "SafulGift"
      tl.fromTo(
        el,
        {
          text: "",
          scale: 0.6,
          opacity: 0,
        },
        {
          text: "SafulGift",
          scale: 1.05,
          opacity: 1,
          duration: duration,
          delay,
          ease,
        }
      );

      tl.to(el, {
        text,
        scale: 1,
        duration: duration * 0.5,
        ease: "power2.out",
      });
    }, containerRef);

    return () => ctx.revert();
  }, [containerRef, text, duration, delay, ease, start]);
}