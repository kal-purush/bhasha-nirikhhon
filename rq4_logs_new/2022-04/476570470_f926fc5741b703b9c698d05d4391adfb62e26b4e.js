(() => {
    let page = document.getElementById("blocks");
    for(let i = 0; i < 50; i++){
      let block = document.createElement("div");
      block.className = "blockPage";
      block.style.opacity = ((i%5)/5) + 0.2;
      block.onmouseover = function(event) {
        event.target.style.backgroundColor = "red";
      }
      block.onmouseout = function(event) {
        event.target.style.removeProperty("background-color");;
      }
      page.appendChild(block);
    }
  })();

  const plusClicked = document.getElementById('plusSign');
  const resetClicked = document.getElementById('resetSign');
  let change1 = document.getElementsByClassName("blockPage")[0];
  let opacityIndicator = 0;
  plusClicked.addEventListener('click', function onClick() {
  resetClicked.style.visibility = "visible";
  change1.style.opacity = opacityIndicator;
  if(opacityIndicator<1) opacityIndicator+=0.1;
  else opacityIndicator = 0;
})

resetClicked.addEventListener('click', function onClick() {
  let change1 = document.getElementsByClassName("blockPage")[0];
  if (change1 == undefined) return;
  change1.style.opacity = 0.2;
  opacityIndicator = 0;
  resetClicked.style.visibility = "hidden";
})