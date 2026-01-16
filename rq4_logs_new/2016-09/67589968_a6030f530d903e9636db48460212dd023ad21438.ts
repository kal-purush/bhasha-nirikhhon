var navbar: Element = document.getElementById("navbar");

var chapters: Array<any> = Array.from(document.getElementsByClassName("chapter"));

for(let i = 0; i < chapters.length; i++){
  let subtitle = chapters[i].firstElementChild.firstElementChild;
  subtitle.setAttribute("id", `${"chapter" + i}`);

  let li: Element = document.createElement("li");
  navbar.appendChild(li);

  let a: Element = document.createElement("a");
  a.setAttribute("href", `${"#chapter" + i}`);
  a.innerHTML = subtitle.innerHTML;
  a.innerHTML = a.innerHTML.split("-")[0];
  li.appendChild(a);

  var parts = chapters[i].getElementsByClassName("part");
  var ul: Element;
  if(parts.length != 0) {
    ul = document.createElement("ul");
    li.appendChild(ul);
  };

  for(let e = 0; e < parts.length; e++){
    parts[e].setAttribute("id", `${"part" + e}`);
    let li: Element = document.createElement("li");
    ul.appendChild(li);

    let a: Element = document.createElement("a");
    a.setAttribute("href", `${"#part" + e}`);
    a.innerHTML = parts[e].firstElementChild.innerHTML;
    li.appendChild(a);
  };
};

var  fixNavBar = function(){
  var navbarRect = navbar.getBoundingClientRect(),
      offset = navbarRect.top - window.screenTop;

      console.log(navbarRect.top);

  if(offset < 0){
    navbar.setAttribute("style", "position:fixed");
  };
};