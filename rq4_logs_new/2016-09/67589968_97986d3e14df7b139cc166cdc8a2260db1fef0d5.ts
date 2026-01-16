var navbar: Element = document.getElementById("navbar");

var chapters: Array<any> = Array.from(document.getElementsByClassName("chapters"));

for(let i: number = 0; i < chapters.length; i++){
  let title = chapters[i].firstElementChild;
  console.log(title);
  title.setAttribute("id", `${"chapter" + i}`);

  let li: Element = document.createElement("li");
  navbar.appendChild(li);

  let a: Element = document.createElement("a");
  a.setAttribute("href", `${"#chapter" + i}`);
  a.innerHTML = title.innerHTML;
  a.innerHTML = a.innerHTML.split("-")[0];
  a.innerHTML = a.innerHTML.split(":")[0];
  li.appendChild(a);

  var parts = chapters[i].getElementsByClassName("sous-titre");
  var ul: Element;
  if(parts.length != 0) {
    ul = document.createElement("ul");
    li.appendChild(ul);
  };

  for(let e: number = 0; e < parts.length; e++){
    parts[e].setAttribute("id", `${"part" + e}`);
    let li: Element = document.createElement("li");
    ul.appendChild(li);

    let a: Element = document.createElement("a");
    a.setAttribute("href", `${"#part" + e}`);
    a.innerHTML = parts[e].innerHTML;
    a.innerHTML = a.innerHTML.split("-")[0];
    a.innerHTML = a.innerHTML.split(":")[0];
    li.appendChild(a);
  };
};