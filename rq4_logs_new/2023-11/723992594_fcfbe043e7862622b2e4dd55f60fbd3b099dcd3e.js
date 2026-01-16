document.getElementById("calculate-btn").addEventListener("click", () => {
  const totalIncome =
    parseInt(document.getElementById("total-income").value) || 0;
  const foodExpenses =
    parseInt(document.getElementById("food-expenses").value) || 0;
  const rentExpenses =
    parseInt(document.getElementById("rent-expenses").value) || 0;
  const clothExpenses =
    parseInt(document.getElementById("cloth-expenses").value) || 0;

  const totalExpenses = foodExpenses + rentExpenses + clothExpenses;
  document.getElementById("total-expenses").textContent = totalExpenses;

  const newBalance = totalIncome - totalExpenses;
  document.getElementById("new-balance").textContent = newBalance;

  if (newBalance < 0) {
    alert(
      "Warning: Your expenses exceed your income. Please review your budget."
    );
  }
});

document.getElementById("save-btn").addEventListener("click", () => {
  let oldAmount = document.getElementById("new-balance").textContent;
  const savingPercent =
    parseInt(document.getElementById("saving-percent").value) || 0;

  const savingAmount = (savingPercent / 100) * parseInt(oldAmount);
  document.getElementById("saving-amount").textContent = savingAmount;

  const remainingAmount =
    document.getElementById("new-balance").textContent - savingAmount;

  if (remainingAmount < 0) {
    alert(
      "Warning: Your saving amount exceeds your remaining balance. Please adjust your savings."
    );
    document.getElementById("saving-amount").textContent = "00";
    document.getElementById("remaining-amount").textContent = "00";
  } else {
    document.getElementById("remaining-amount").textContent = remainingAmount;
  }
});