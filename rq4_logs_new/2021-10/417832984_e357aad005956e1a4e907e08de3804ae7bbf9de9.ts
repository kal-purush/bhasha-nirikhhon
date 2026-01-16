import { PATHS_CORE } from "common/constants/paths";
import { fallbackLng } from "locales";
import { useTranslation } from "react-i18next";

const validateSlashBeforePath = (path: string) => {
  return path === "" ? path : path[0] === "/" ? path : `/${path}`;
};

const useLocalizedPath = () => {
  const { i18n } = useTranslation();

  const getLocalizedPath = (path: string, lang?: string) => {
    const correctLang = lang || i18n.language;

    const correctPath = path === PATHS_CORE.HOMEPAGE ? "" : path;

    return correctLang === fallbackLng
      ? path
      : `/${correctLang}${validateSlashBeforePath(correctPath)}`;
  };

  return {
    path: getLocalizedPath,
  };
};

export default useLocalizedPath;