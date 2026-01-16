// import { useEffect, useRef, useCallback, useState } from "react";
// import { useLocation } from "react-router";
// import { gsap } from "gsap";
// import { ScrollToPlugin } from "gsap/ScrollToPlugin";

// gsap.registerPlugin(ScrollToPlugin);

// export function useScrollHandler() {
//   const location = useLocation();
//   const [activeSection, setActiveSection] = useState("");
//   const scrollStartY = useRef(0);
//   const scrollTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
//   const isHomePage = location.pathname === "/";

//   const scrollToSection = useCallback(
//     (
//       sectionId: string,
//       offset = 120,
//       autoKill = true,
//       onComplete?: () => void
//     ) => {
//       gsap.to(window, {
//         duration: 1.5,
//         scrollTo: { y: `#${sectionId}`, offsetY: offset, autoKill },
//         ease: "power3.inOut",
//         onComplete: () => {
//           setActiveSection(sectionId);
//           onComplete?.();
//         },
//       });
//     },
//     []
//   );

//   // Handle auto-scroll on homepage
//   useEffect(() => {
//     if (!isHomePage) return;

//     const handleScroll = (e: WheelEvent) => {
//       if (window.scrollY > 300) return;
//       if (Math.abs(e.deltaY) < Math.abs(e.deltaX)) return;

//       if (scrollTimeout.current) {
//         clearTimeout(scrollTimeout.current);
//       }

//       if (scrollStartY.current === 0) {
//         scrollStartY.current = window.scrollY;
//       }

//       const scrollThreshold = 100;
//       const currentScroll = window.scrollY;
//       const scrollDiff = currentScroll - scrollStartY.current;

//       if (Math.abs(scrollDiff) >= scrollThreshold) {
//         scrollToSection("features", 280, false);
//         scrollStartY.current = 0;
//       } else {
//         scrollTimeout.current = setTimeout(() => {
//           scrollStartY.current = 0;
//         }, 300);
//       }
//     };

//     window.addEventListener("wheel", handleScroll, { passive: false });

//     return () => {
//       window.removeEventListener("wheel", handleScroll);
//       if (scrollTimeout.current) {
//         clearTimeout(scrollTimeout.current);
//       }
//     };
//   }, [isHomePage, scrollToSection]);

//   return { isHomePage, activeSection, scrollToSection };
// }

// TYPE 2

import { useEffect, useRef, useState } from "react";
import { useLocation } from "react-router";
import { gsap } from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";
import { ScrollToPlugin } from "gsap/ScrollToPlugin";

gsap.registerPlugin(ScrollTrigger, ScrollToPlugin);

export function useSmoothScroll() {
  const location = useLocation();
  const [activeSection, setActiveSection] = useState("");
  const isHomePage = location.pathname === "/";
  const scrollTween = useRef<gsap.core.Tween | null>(null);
  const scrollToRef = useRef(
    (location.state as { scrollTo?: string })?.scrollTo
  );

  // Smooth scroll function

  const scrollToSection = (
    sectionId: string,
    offset = 0,
    autoKill = true,
    forceUpdate = false
  ) => {
    scrollTween.current?.kill();
    scrollTween.current = gsap.to(window, {
      duration: 1.2,
      scrollTo: { y: `#${sectionId}`, offsetY: offset, autoKill },
      ease: "power2.inOut",
      onComplete: () => {
        setActiveSection(sectionId);
        if (forceUpdate) window.history.replaceState({}, "");
      },
      onInterrupt: () => (scrollTween.current = null),
    });
  };

  // Handle scroll target from navigation
  useEffect(() => {
    if (scrollToRef.current) {
      const element = document.getElementById(scrollToRef.current);
      if (element) {
        scrollToSection(scrollToRef.current, 120, true, true);
      }
      scrollToRef.current = undefined;
    }
  }, [location.pathname]);

  //   useEffect(() => {
  //     if (!isHomePage) return;

  //     const sections = gsap.utils.toArray<HTMLElement>("[data-section]");

  //     sections.forEach((section) => {
  //       ScrollTrigger.create({
  //         trigger: section,
  //         start: "top center",
  //         end: "bottom center",
  //         onEnter: () => setActiveSection(section.id),
  //         onEnterBack: () => setActiveSection(section.id),
  //       });
  //     });

  //     // Handle home section (when scrolled to top)
  //     ScrollTrigger.create({
  //       trigger: "body",
  //       start: "top top",
  //       end: "max",
  //       onUpdate: (self) => {
  //         if (self.scroll() === 0) setActiveSection("home");
  //       },
  //     });

  //     return () => {
  //       ScrollTrigger.getAll().forEach((trigger) => trigger.kill());
  //     };
  //   }, [isHomePage]);

  // Section detection setup

  useEffect(() => {
    if (!isHomePage) {
      setActiveSection("");
      return;
    }

    const sections = gsap.utils.toArray<HTMLElement>("[data-section]");

    const observers = sections.map((section) => {
      return ScrollTrigger.create({
        trigger: section,
        start: "top center",
        end: "bottom center",
        onToggle: (self) => {
          if (self.isActive) setActiveSection(section.id);
        },
      });
    });

    // Home section detection
    const homeObserver = ScrollTrigger.create({
      trigger: "body",
      start: "top top",
      onEnter: () => setActiveSection("home"),
      onEnterBack: () => setActiveSection("home"),
    });

    return () => {
      [...observers, homeObserver].forEach((obs) => obs?.kill());
    };
  }, [isHomePage]);

  useEffect(() => {
    if (!isHomePage) return;

    let isScrolling = false;

    const handleWheel = (e: WheelEvent) => {
      if (window.scrollY > 300 || isScrolling) return;
      if (Math.abs(e.deltaY) < Math.abs(e.deltaX)) return;

      isScrolling = true;

      if (e.deltaY > 0) {
        scrollToSection("features", 280, false);
      }

      setTimeout(() => (isScrolling = false), 1000);
    };

    window.addEventListener("wheel", handleWheel, { passive: false });
    return () => window.removeEventListener("wheel", handleWheel);
  }, [isHomePage]);

  return { isHomePage, activeSection, setActiveSection, scrollToSection };
}