import React, { useEffect, useRef, useState } from "react";
import "./Animations.css"; // Importamos las animaciones CSS

const Animations = ({ children, animationType }) => {
   const elementRef = useRef(null);
   const [isVisible, setIsVisible] = useState(false);

   useEffect(() => {
      const observer = new IntersectionObserver(
         (entries) => {
            entries.forEach((entry) => {
               if (entry.isIntersecting) {
                  setIsVisible(true);
               } else {
                  setIsVisible(false); // Reinicia la visibilidad
               }
            });
         },
         { threshold: 0.1 } // Se activa si al menos el 10% del elemento es visible
      );

      if (elementRef.current) {
         observer.observe(elementRef.current);
      }

      // Cleanup del observer
      return () => {
         if (elementRef.current) observer.unobserve(elementRef.current);
      };
   }, []);

   return (
      <div
         ref={elementRef}
         className={`animated-element ${isVisible ? animationType : ""}`}
      >
         {children}
      </div>
   );
};

export default Animations;