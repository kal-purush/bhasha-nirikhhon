chrome.storage.local.get(["symbolSet"], function(data){
  if(data.symbolSet === undefined)
    chrome.storage.local.set({'symbolSet': DEFAULT_SET});
});

function openOptionsOnFirstInstalling() {
    if (!localStorage.getItem('was_installed')){
      localStorage.setItem('was_installed', true);
      chrome.tabs.create({ "url": "chrome-extension://" + chrome.runtime.id + "/options/index.html"});
    }
}
setTimeout(openOptionsOnFirstInstalling, 500);

chrome.runtime.onMessage.addListener(function(data, sender){
  switch(data.eve){
    case 'to_destroy_child':
      chrome.tabs.sendMessage(sender.tab.id, {eve: 'to_destroy_child'});
      break;
  };
});