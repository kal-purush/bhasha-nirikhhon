function ChosenType(type)
{
  let div = document.getElementById("formPlace");
  let btnDiv = document.getElementById("sendBtn");
  div.innerHTML = '';
  div.innerHTML += `<div class="uploadItem"><label for="name">Termék neve: </label> <input type="text" name="name" id="name"></div> <div class="uploadItem"><label for="manufacturer">Gyártó: </label> <input type="text" name="manufacturer" id="manufacturer"></div> <div class="uploadItem"><label for="price">Termék ára: </label> <input type="number" name="price" id="price"></div>`;
  switch (type) {
    case "CPU":
      div.innerHTML += `<div class="uploadItem"><label for="socket">CPU foglalat: </label> <input type="text" name="socket" id="socket"></div> <div class="uploadItem"><label for="maxram">Maximális támogatott memória: </label> <input type="number" name="maxram" id="maxram"></div> <div class="uploadItem"><label for="ddrtype">Támogatott DDR típus: </label> <input type="text" name="ddrtype" id="ddrtype"></div> <div class="uploadItem"><label for="corenum">Magok száma: </label> <input type="number" name="corenum" id="corenum"></div> <div class="uploadItem"><label for="threadnum">Szálak száma: </label> <input type="number" name="threadnum" id="threadnum"></div> <div class="uploadItem"><label for="clockspeed">Alap órajel: </label> <input type="number" name="clockspeed" id="clockspeed"></div> <div class="uploadItem"><label for="turboclockspeed">Turbo órajel: </label> <input type="number" name="turboclockspeed" id="turboclockspeed"></div>`;
      break;
  
    default:
      break;
  }
  div.innerHTML += `<div class="uploadItem"><label for="stock">Készlet: </label> <input type="number" name="stock" id="stock"></div>`;
  btnDiv.innerHTML = ``;
}