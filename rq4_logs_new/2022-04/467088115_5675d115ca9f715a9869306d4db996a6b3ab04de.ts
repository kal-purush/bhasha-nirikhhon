import { useEffect } from 'react';
import { ThemeState } from "stores/themeAtom";
import { useRecoilState } from 'recoil';


export const useTheme = () => {
    const [darkMode, setDarkMode] = useRecoilState<boolean>(ThemeState);


    const chekDarkmode = (): void => {
        const isBrowserDarkMode = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        const localSettingDarkMode = localStorage.getItem('darkMode');

        if (localSettingDarkMode) {
            setDarkMode(localSettingDarkMode == 'false' ? false : true);
        } else {
            setDarkMode(isBrowserDarkMode)
        }
    }

    useEffect(() => {
        chekDarkmode();
    }, [])

    const handleDarkMode = (): void => {
        localStorage.setItem('darkMode', '' + !darkMode)
        setDarkMode(!darkMode);
    }

    return {
        handleDarkMode,
        darkMode
    }
};