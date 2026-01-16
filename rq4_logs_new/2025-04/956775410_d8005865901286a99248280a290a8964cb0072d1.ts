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
  const scrollTween = useRef<gsap.core.Tween | null>(null);
  const scrollToRef = useRef(
    (location.state as { scrollTo?: string })?.scrollTo
  );

  const isHomePage = location.pathname === "/";
  const isHeroSection =
    isHomePage &&
    (window.scrollY < 100 ||
      !activeSection ||
      activeSection === "home" ||
      activeSection === "");

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

  useEffect(() => {
    if (!isHomePage) return;

    const sections = gsap.utils.toArray<HTMLElement>("[data-section]");

    sections.forEach((section) => {
      ScrollTrigger.create({
        trigger: section,
        start: "top center",
        end: "bottom center",
        onEnter: () => setActiveSection(section.id),
        onEnterBack: () => setActiveSection(section.id),
      });
    });

    // Handle home section (when scrolled to top)
    ScrollTrigger.create({
      trigger: "body",
      start: "top top",
      end: "max",
      onUpdate: (self) => {
        if (self.scroll() === 0) setActiveSection("home");
      },
    });

    return () => {
      ScrollTrigger.getAll().forEach((trigger) => trigger.kill());
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

  return {
    isHomePage,
    isHeroSection,
    activeSection,
    setActiveSection,
    scrollToSection,
  };
}