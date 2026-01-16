money = 10000000000;
moneyup = 1;
msec = 1 ;
upcost = 15;
catcost = 25;
Messicost = 50000000;
upown = 0;
catown = 0;
Messiown = 0;
catadd = 1;
workadd = 1000000;
cboost = 1;
wboost = 1;
catmax = 0;
workmax = 0;

function closingCode() {
  if (confirm("Voulez vous sauvegarder votre partie ?") === true) {
    save();
    return null;
  }
}

function addcomma(x) {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
}
function reloadall() {
  document.getElementById("click").innerHTML =
    "€/click: " + addcomma(moneyup) + " | €/sec: " + addcomma(msec);
  document.getElementById("total").innerHTML = "Capital : " + addcomma(money);
  document.getElementById("cat").innerHTML =
    catown + "Entrainement " + addcomma(catcost) + " | +" + addcomma(catadd) + "/sec";
  document.getElementById("Messi").innerHTML =
    Messiown + ": " + addcomma(Messicost) + " | +" + addcomma(workadd) + "/sec";
  document.getElementById("upgrade").innerHTML =
    addcomma(upown) + "-main upgrade: " + addcomma(upcost);
}
function save() {
  localStorage.setItem("money", money);
  localStorage.setItem("moneyup", moneyup);
  localStorage.setItem("msec", msec);
  localStorage.setItem("upcost", upcost);
  localStorage.setItem("catcost", catcost);
  localStorage.setItem("catadd", catadd);
  localStorage.setItem("Messicost", Messicost);
  localStorage.setItem("workadd", workadd);
  localStorage.setItem("catown", catown);
  localStorage.setItem("Messiown", Messiown);
  localStorage.setItem("upown", upown);
  localStorage.setItem("catadd", catadd);
  localStorage.setItem("workadd", workadd);
  localStorage.setItem("cboost", cboost);
  localStorage.setItem("wboost", wboost);
  localStorage.setItem("catmax", catmax);
  localStorage.setItem("workmax", workmax);
}
function load() {
  money = parseInt(localStorage.getItem("money"));
  moneyup = parseInt(localStorage.getItem("moneyup"));
  msec = parseInt(localStorage.getItem("msec"));
  upcost = parseInt(localStorage.getItem("upcost"));
  catcost = parseInt(localStorage.getItem("catcost"));
  upown = parseInt(localStorage.getItem("catadd"));
  Messicost = parseInt(localStorage.getItem("Messicost"));
  upown = parseInt(localStorage.getItem("workadd"));
  catown = parseInt(localStorage.getItem("catown"));
  Messiown = parseInt(localStorage.getItem("Messiown"));
  upown = parseInt(localStorage.getItem("upown"));
  catadd = parseInt(localStorage.getItem("catadd"));
  workadd = parseInt(localStorage.getItem("workadd"));
  cboost = parseInt(localStorage.getItem("cboost"));
  wboost = parseInt(localStorage.getItem("wboost"));
  catmax = parseInt(localStorage.getItem("catmax"));
  workmax = parseInt(localStorage.getItem("workmax"));

  reloadall();
}
function reset() {
  if (confirm("Etes vous sur de réinitialiser ?") === true) {
    money = 0;
    moneyup = 1;
    msec = 0;
    upcost = 15;
    catcost = 25;
    Messicost = 250;
    catown = 0;
    Messiown = 0;
    upown = 0;
    catadd = 1;
    workadd = 15;
    reloadall();
  }
}
function myTimer() {
    money += msec;
  document.getElementById("total").innerHTML = "Capital : " + addcomma(money) + " €";
}
setInterval(myTimer, 1000);

function clicked() {
  money += moneyup;
  document.getElementById("total").innerHTML = "Capital : " + addcomma(money) + " € ";
}
function upgrade(name) {
  if (name == "Entrainement") {
    if (money >= catcost && catown < 50) {
      
      if (catown <= 13) {
        msec += catadd;
        catadd++;
        cboost = 1;
      } else if (catown == 14) {
        msec += catadd;
        catadd++;
        cboost = 200;
      } else if (catown <= 23) {
        msec += 200 * catadd;
        catadd++;
        cboost = 200;
      } else if (catown == 24) {
        msec += 200 * catadd;
        catadd++;
        cboost = 5000;
      } else if (catown <= 48) {
        msec += 5000 * catadd;
        catadd++;
        cboost = 5000;
      } else if (catown == 49) {
        msec += 5000 * catadd;
        catadd++;
        cboost = 15000;
      } else {
        msec += 15000 * catadd;
        catadd++;
        cboost = 15000;
      }
      catown += 1;
      money -= catcost;
      catcost = catcost * 2;
      document.getElementById("cat").innerHTML =
        catown + "Entrainement " + addcomma(catcost) + " | +" + addcomma(catadd * cboost) + "/sec";
    } else if (catown == 50) {
      document.getElementById("cat").innerHTML =
        catown + "Entrainement MAX | +15% click/sec";
    }
  }

  if (name == "Messi") {
    if (money >= Messicost && Messiown < 50) {
      
      if (Messiown <= 13) {
        msec += workadd;
        workadd++;
        wboost = 1;
      } else if (Messiown == 14) {
        msec += workadd;
        workadd++;
        wboost = 200;
      } else if (Messiown <= 23) {
        msec += 200 * workadd;
        workadd++;
        wboost = 200;
      } else if (Messiown == 24) {
        msec += 200 * workadd;
        workadd++;
        wboost = 5000;
      } else if (Messiown <= 48) {
        msec += 5000 * workadd;
        workadd++;
        wboost = 5000;
      } else if (Messiown == 49) {
        msec += 5000 * workadd;
        workadd++;
        wboost = 15000;
      } else {
        msec += 15000 * workadd;
        workadd++;
        wboost = 15000;
      }
      Messiown += 1;
      money -= Messicost;
      Messicost = Messicost * 3;
      document.getElementById("Messi").innerHTML = 
      Messiown + "Messi: " + addcomma(Messicost) + " | +" + addcomma(workadd * wboost) + "/sec";
    } else if (Messiown == 50) {
      document.getElementById("Messi").innerHTML =
      Messiown + "Messi: MAX | +35% click/sec";
    }
  }

  if (name == "upgrade") {
    if (money >= upcost) {
      moneyup += upcost / 15;
      money -= upcost;
      upown += 1;
      upcost = upcost * 5;
      document.getElementById("upgrade").innerHTML =
        addcomma(upown) + "-main upgrade: " + addcomma(upcost);
      if (catown == 50) {
        msec -= catmax;
        catmax = Math.floor(moneyup * 0.15);
        msec += catmax;
      }
      if (Messiown == 50) {
        msec -= workmax;
        workmax = Math.floor(moneyup * 0.35);
        msec += workmax;
      }
    }
  }

  document.getElementById("click").innerHTML =
    "€/click: " + addcomma(moneyup) + " | €/sec: " + addcomma(msec); 
  document.getElementById("total").innerHTML = "Capital : " + addcomma(money) + " €";
}