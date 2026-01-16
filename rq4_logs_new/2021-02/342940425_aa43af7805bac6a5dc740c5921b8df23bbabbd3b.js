
class Speaker{
  
  synUtterance;
  blocked = false;
  host = "wooordhunt.ru"
  speachVoiceIndex = 11;

  constructor()
  {
    this.synUtterance = new SpeechSynthesisUtterance();
  }

  main()
  {
    if (location.host == this.host){
      this.setSettings();
      this.drowDomElements();
      this.bindEvents();
      this.appendStyle();
    }
  }

  bindEvents()
  {
    let playElements = document.querySelectorAll('.audio-play');
    for(let i = 0 ; i < playElements.length; i++){
      if (!playElements[i].addEventListener !== undefined) {
        playElements[i].addEventListener("click", (e)=>{
          this.speak(playElements[i].dataset.text);
        });
      }
    }
  }

  async setSettings()
  {
    let voices = await this.initVoices();
    this.synUtterance.voice = voices[this.speachVoiceIndex]
    this.synUtterance.lang = 'en-US';
  }

  updateElementsClass()
  {
    let playElements = document.querySelectorAll('.audio-play');
    for(let i = 0 ; i < playElements.length; i++){
      if (this.blocked) {
        playElements[i].classList.add('blocked')
      } else {
        playElements[i].classList.remove('blocked');
      }
    }
  }

  appendStyle()
  {
    let styleString = 
    `<style>
      .audio-play{
        color:red;
        font-weight: 600;
        margin-right: 5px;
      }
      .audio-play.blocked{
        opacity: 0.5;
      }
    </style>`;
    document.head.insertAdjacentHTML("beforeend", styleString);
  }

  drowDomElements()
  {
    let exNodes = document.querySelectorAll('#content_in_russian .ex i')
    for(let i = 0; i < exNodes.length; i++){
      let textNode = exNodes[i].previousSibling;
      let spanNode = this.createPlayElement(textNode.nodeValue)
      exNodes[i].prepend(spanNode);
    }

    //phrase verbs
    let phrNodes = document.querySelectorAll('#content_in_russian .phrases i')
    for(let i = 0; i < phrNodes.length; i++){
      let text = '';
      let textPharsesNodes = this.getPreviousSiblingsExceptI(phrNodes[i]);
      for(let j = textPharsesNodes.length - 1; j >= 0; j--){
        console.log(j);
        if (textPharsesNodes[j].nodeType == 3){
          text+=textPharsesNodes[j].textContent;
        } else {
          text+=textPharsesNodes[j].innerText;
        }
      }

      let spanNode = this.createPlayElement(text)
      phrNodes[i].prepend(spanNode);
      text = '';
    }

    //Examples
    let ex_oNodes = document.querySelectorAll('#content_in_russian .ex_o');
    for(let i = 0; i < ex_oNodes.length; i++){
      let textNode = ex_oNodes[i].childNodes[0];
      let spanNode = this.createPlayElement(textNode.nodeValue)
      ex_oNodes[i].append(spanNode);
    }
  }

  getPreviousSiblingsExceptI(elem, filter) {
    var sibs = [];
    while (elem = elem.previousSibling) {
        if (elem.tagName == 'I') break;
        if (!filter || filter(elem)) sibs.push(elem);
    }
    return sibs;
}
  createPlayElement(text)
  {
    let spanNode = document.createElement("span");
    spanNode.innerHTML = "[Play]";
    spanNode.classList.add('audio-play');
    spanNode.dataset.text = text.replace(/[^a-zA-Z ]/g, "");

    return spanNode;
  }

  speak(text)
  {
    if (this.blocked) return false;

    this.synUtterance.text = text;
    const eventList = ["start", "end", "mark", "pause", "resume", "error", "boundary"];

    eventList.forEach((event) => {
      this.synUtterance.addEventListener(event, (speechSynthesisEvent) => {
        // console.log(`Fired '${speechSynthesisEvent.type}' event at time '${speechSynthesisEvent.elapsedTime}' and character '${speechSynthesisEvent.charIndex}'.`);
        if (speechSynthesisEvent.type == 'start')
        {
          this.blocked = true;
          this.updateElementsClass();
        } else if(speechSynthesisEvent.type == 'end'){
          this.blocked = false;
          this.updateElementsClass();
        }
      });
    });
    
    window.speechSynthesis.speak(this.synUtterance);
  }

  async initVoices() 
  {
    return new Promise(
        function (resolve, reject) {
            let synth = window.speechSynthesis;
            let id;
  
            id = setInterval(() => {
                if (synth.getVoices().length !== 0) {
                    resolve(synth.getVoices());
                    clearInterval(id);
                }
            }, 10);
        }
    )
  }
}

let s = new Speaker()
s.main();