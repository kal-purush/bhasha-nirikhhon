document.addEventListener("DOMContentLoaded", async () => {
  const selftitledCheck = document.getElementById("selfTitledToggle");

  const selftitledResult = await browser.storage.local.get("replaceWithSelfTitled");
  selftitledCheck.checked = selftitledResult.replaceWithSelfTitled ?? true;

  selftitledCheck.addEventListener("change", () => {
    browser.storage.local.set({
      replaceWithSelfTitled: selftitledCheck.checked
    });
  });

  document.getElementById("saveBtn").addEventListener("click", () => {
    window.close();
  });
});