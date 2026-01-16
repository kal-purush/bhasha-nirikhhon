import { data } from '../data/words';

export class AppApi {
  getRandomWord = async () => {
    const randomGenre = Math.round(Math.random() * (data.themes.length - 1));
    const randomWord = Math.round(Math.random() * (data.themes[randomGenre].words.length - 1));

    return Promise.resolve({ data: data.themes[randomGenre].words[randomWord].toLowerCase() });
  };

  getThemeWord = async (theme: string) => {
    const themeObj = data.themes.find((element) => element.name === theme);

    if (themeObj) {
      const random = Math.round(Math.random() * (themeObj.words.length - 1));

      return Promise.resolve({ data: themeObj.words[random]?.toLowerCase() });
    }

    return Promise.reject(new Error('тема не найдена'));
  };
}

export const appApi = new AppApi();