let newsPage = document.getElementById("news") as HTMLDivElement;

for (let g = 0; g < shopItemsData.length; g++) {
    const element = shopItemsData[g];
    let newsContainer = document.createElement("div") as HTMLDivElement;
    newsContainer.setAttribute ("id", "newsContainer");
   newsPage.append(newsContainer);
}