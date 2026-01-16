import { useEffect } from "react";
import { gsap } from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";

export const useScrollTriggers = (
  isActive: boolean,
  setActiveSection: (id: string) => void
) => {
  useEffect(() => {
    if (!isActive) return;

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

    // Home section trigger
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
  }, [isActive, setActiveSection]);
};