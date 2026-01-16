"use client"; // Ensures this runs only on the client side

import { useTheme } from "next-themes";
import { useEffect, useState } from "react";
import { Sun, Moon } from "lucide-react";

const ThemeSwitcher = () => {
    const { theme, setTheme, resolvedTheme } = useTheme();
    const [mounted, setMounted] = useState(false);

    // Ensure the component is mounted before accessing the theme
    useEffect(() => {
        setMounted(true);
    }, []);

    if (!mounted) return null; // Prevents hydration errors

    return (
        <button
            onClick={() => setTheme(resolvedTheme === "light" ? "dark" : "light")}
            className="relative flex items-center w-[48px] h-[24px] px-1 rounded-full cursor-pointer bg-gray-300 dark:bg-gray-700 transition-colors"
        >
            <Sun
                size={14}
                className={`absolute left-[6px] transition-opacity ${resolvedTheme === "dark" ? "opacity-30" : "opacity-100"}`}
            />
            <Moon
                size={14}
                className={`absolute right-[6px] transition-opacity ${resolvedTheme === "dark" ? "opacity-100" : "opacity-30"}`}
            />
            <div
                className={`absolute size-[18px] bg-white dark:bg-gray-900 rounded-full shadow-md transition-transform ${resolvedTheme === "dark" ? "translate-x-[22px]" : "translate-x-0"
                    }`}
            ></div>
        </button>
    );
};

export default ThemeSwitcher;