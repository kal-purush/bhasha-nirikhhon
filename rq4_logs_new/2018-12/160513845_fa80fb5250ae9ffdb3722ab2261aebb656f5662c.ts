chrome.runtime.onMessage.addListener(request => {
  if (request.type === "getPlaylist") {
    document.body.innerHTML += `<dialog style="height:40%">
    <iframe id="headlineFetcher"style="height:100%"></iframe>
    <div style="position:absolute; top:0px; left:5px;">  
      <button>x</button>
    </div>
    </dialog>`;
    const dialog = document.querySelector("dialog");
    dialog.showModal();
  }
});