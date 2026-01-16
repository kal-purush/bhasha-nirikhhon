const list = [];

// Zapisz sobie referencje do elementów HTML
// Nie zmieniając pliku `index.html`
const ref = {
  form: undefined,
  itemsList: undefined,
  addItemButton: undefined
};

/**
 * Dodaj nowy element do podanej tablicy
 * @param {object[]} targetArray
 * @param {string} name
 * @param {boolean=false} status
 */
function addItem(targetArray, name, status = false) {
  //
}

/**
 * Zapisuje cały obiekt w LocalStorage, pod kluczem 'items'
 * @param {object[]} array
 */
function rememberList(array) {
  //
}

/**
 * Wyświetl tablicę rzeczy w docelowym elemencie HTML
 * @param {object[]} arrayOfItems
 * @param {HTMLElement} targetElement
 */
function renderList(arrayOfItems, targetElement) {
  /**
   * Każdy obiekt ma nazwę i status
   * Wyświetl je w <ul> (unordered list), jako osobne elementy <li> (list item).
   * Status ma być zaprezentowany jako "checkbox"
   */
}

ref.form.addEventListener("submit", event => {
  // event.preventDefault();
  /**
   *
   */
  alert("Próbujesz coś dodać?");
});

/**
 * DODATKOWE PUNKTY
 * Dodaj możliwość usuwania elementów z listy.
 * - array.filter()
 */