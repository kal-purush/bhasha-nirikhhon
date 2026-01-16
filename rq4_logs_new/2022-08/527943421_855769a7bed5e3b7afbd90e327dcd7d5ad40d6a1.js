const countCategories = document.querySelectorAll("#categories > .item");
console.log(`Number of categories: ${countCategories.length}`);

const elements = countCategories.forEach((element) => {
  const contentFromH2 = element.firstElementChild.textContent;
  const countChildrenFromElement = element.lastElementChild.children.length;
  console.log(
    `Category: ${contentFromH2} \nElements: ${countChildrenFromElement}`
  );
});