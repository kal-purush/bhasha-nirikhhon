import { SearchByJenre } from "./fetch.js";

export function jenreBtnClickEvent(key, container) {
  container.addEventListener("click", () => {
    SearchByJenre(key);
  });
}

export const cardClickEvent = (movie, container) => {
  container.addEventListener("click", () => {
    alert(`
        영화 id : ${movie.id}
        영화 제목: ${movie.title}`);
  });
};