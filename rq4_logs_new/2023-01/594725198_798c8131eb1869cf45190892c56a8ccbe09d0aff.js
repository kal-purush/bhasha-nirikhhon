const CATEGORIES = [
  { name: "technology", color: "#3b82f6" },
  { name: "science", color: "#16a34a" },
  { name: "finance", color: "#ef4444" },
  { name: "society", color: "#eab308" },
  { name: "entertainment", color: "#db2777" },
  { name: "health", color: "#14b8a6" },
  { name: "history", color: "#f97316" },
  { name: "news", color: "#8b5cf6" },
];

//selecting DOM-elements
const btn = document.querySelector(".btn-open");
const form = document.querySelector(".fact-form");
const factsList = document.querySelector(".facts-list");

//create DOM-elements
factsList.innerHTML = "";

//load data from Supabase
loadFacts();

async function loadFacts() {
  const res = await fetch(
    "https://cgjimtwnmnspqltboowq.supabase.co/rest/v1/tools",
    {
      headers: {
        apikey:
          "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNnamltdHdubW5zcHFsdGJvb3dxIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzUwNjU5MDMsImV4cCI6MTk5MDY0MTkwM30.AFWFB6vviV-0PyQAYcWXcLNHs_OKhh5oPrvW4ha7ZXs",
        authorisation:
          "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNnamltdHdubW5zcHFsdGJvb3dxIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzUwNjU5MDMsImV4cCI6MTk5MDY0MTkwM30.AFWFB6vviV-0PyQAYcWXcLNHs_OKhh5oPrvW4ha7ZXs",
      },
    }
  );
  const data = await res.json();
  //const filteredData = data.filter((fact) => fact.category === "society");
  //console.log(data);
  createFactsList(data);
}

function createFactsList(dataArray) {
  const htmlArr = dataArray.map(
    (fact) => `<li class="fact"><p>
    ${fact.text}
    <a
        class="source"
        href=${fact.source}
        target="_blank"
        >(Source)</a
    >
    </p>
    <span class="tag" style="background-color: ${
      CATEGORIES.find((cat) => cat.name === fact.category).color
    }"
    >${fact.category}</span
    >
    </li>`
  );
  const html = htmlArr.join("");
  factsList.insertAdjacentHTML("afterbegin", html);
}

//toggle form-visibility
btn.addEventListener("click", function () {
  if (form.classList.contains("hidden")) {
    form.classList.remove("hidden");
    btn.textContent = "Close";
  } else {
    form.classList.add("hidden");
    btn.textContent = "Share a fact";
  }
});