const categoriesEl = document.querySelector('#categories');
const listItemsEls = categoriesEl.children;

console.log(`Number of categories: ${listItemsEls.length}`);

[...listItemsEls].forEach((el) => {
    console.log(`Category: ${el.firstElementChild.textContent}`);
    console.log(`Elements: ${el.lastElementChild.children.length} `)
})