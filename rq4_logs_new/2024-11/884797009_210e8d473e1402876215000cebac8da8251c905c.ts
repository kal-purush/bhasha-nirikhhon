import { useEffect, useState } from "react";

export const useDarkMode = () => {
    const [isDarkMode, setIsDarkMode] = useState(false);

    const toggleDarkMode = (bool?: boolean) => {
        const value = bool ?? !isDarkMode;
        localStorage.setItem("isDarkMode", JSON.stringify(value));
        setIsDarkMode(value);
    }

    useEffect(() => {
        const darkMode = window.matchMedia('(prefers-color-scheme: dark)').matches;
        toggleDarkMode(darkMode);
    }, []);

    useEffect(() => {
        const root = window.document.documentElement;
        root.classList.toggle("dark", isDarkMode);
    }, [isDarkMode]);

    return { isDarkMode, toggleDarkMode };
};