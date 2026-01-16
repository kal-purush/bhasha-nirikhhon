const a = new URLSearchParams(window.location.search);
const t = a.get("target")
console.log("[CORE] [XXXX] Attempting to validate...");
console.info("[CORE] [VALI] Attempting to validate " + t + ".");
if (localStorage.getItem(t) === "unlocked") {
  window.location.href = "./" + t;
} else if ((t === "achievements") && (localStorage.getItem("achieved") === "true")) {
    localStorage.setItem("achievements", "unlocked");
    window.location.href = "./achievements.html";
}
else {
    console.error("[CORE] [VALI] Validation failed for " + t + ".");
    window.alert("Whoops! You haven't unlocked this part of the map yet...");
    history.back();
}