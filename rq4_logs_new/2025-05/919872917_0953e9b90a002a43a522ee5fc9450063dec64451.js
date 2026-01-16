const softwareData = [
  {
    name: "素材1",
    file: "test1.psd",
    description: "素材1描述",
    page: "resource/test1.html",
    thumbnail: "images/software1.jpg",
  },
  {
    name: "素材2",
    file: "test2.psd",
    description: "素材2描述",
    page: "resource/test2.html",
    thumbnail: "images/software2.jpg",
  },
  {
    name: "素材3",
    file: "test3.psd",
    description: "素材3描述",
    page: "resource/test3.html",
    thumbnail: "images/software3.jpg",
  },
];

// 首页搜索框的处理
function searchSoftware() {
  const query = document.getElementById("searchInput").value.trim();
  if (query) {
    // 跳转到结果页面，传递查询参数
    window.location.href = `results.html?query=${encodeURIComponent(query)}`;
  }
}

// 搜索结果页面的处理
window.onload = function () {
  const query = new URLSearchParams(window.location.search).get("query");
  if (query) {
    const filteredSoftware = softwareData.filter((software) =>
      software.name.toLowerCase().includes(query.toLowerCase())
    );
    displaySoftwareList(filteredSoftware);
  }
};

function displaySoftwareList(softwareList) {
  const listElement = document.getElementById("softwareList");
  listElement.innerHTML = ""; // 清空当前列表

  if (softwareList.length === 0) {
    listElement.innerHTML = "<li>没有找到匹配的软件。</li>";
  } else {
    softwareList.forEach((software) => {
      const listItem = document.createElement("li");

      // 创建缩略图元素
      const thumbnail = document.createElement("img");
      thumbnail.src = software.thumbnail;
      thumbnail.alt = software.name + "缩略图";
      thumbnail.classList.add("software-thumbnail");

      // 创建软件名称文本
      const nameText = document.createElement("span");
      nameText.textContent = software.name;

      // 将缩略图和文本添加到列表项
      listItem.appendChild(thumbnail);
      listItem.appendChild(nameText);

      // 点击跳转到软件详情页面
      listItem.onclick = () => {
        window.location.href = software.page;
      };

      listElement.appendChild(listItem);
    });
  }
}

// 回车按键搜索
document
  .getElementById("searchInput")
  .addEventListener("keydown", function (event) {
    if (event.key === "Enter") {
      searchSoftware();
    }
  });