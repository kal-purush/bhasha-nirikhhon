import { useState, useEffect } from "react";
import breakpoints from "@/data/breakpoints.json";

export function useBreakpoint() {
    const [width, setWidth] = useState<number>(0);

    useEffect(() => {
        if (typeof window !== "undefined") {
            const handleResize = () => setWidth(window.innerWidth);
            handleResize();
            window.addEventListener("resize", handleResize);
            return () => window.removeEventListener("resize", handleResize);
        }
    }, []);

    return {
        isMobile: width < breakpoints.sm,
        isTablet: width >= breakpoints.sm && width < breakpoints.lg,
        isDesktop: width >= breakpoints.lg,
        isLargeDesktop: width >= breakpoints.xl,
    };
}