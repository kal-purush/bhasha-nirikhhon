const backdrop = document.querySelector(".backdrop");
const model = document.querySelector(".modal");
const selectPlanButtons = document.querySelectorAll(".plan .button");

if (selectPlanButtons && selectPlanButtons.length > 0) {
  selectPlanButtons.forEach(item =>
    item.addEventListener("click", () => {
      model.style.display = "block";
      backdrop.style.display = "block";
    })
  );
}