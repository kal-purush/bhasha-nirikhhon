let customStyle = document.querySelector(".explorer-folders-view .monaco-list-rows");
let observer = new MutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
        if (mutation.type == "attributes") {
            if (mutation.target.dataset.top !== mutation.target.style.top) {
                let newtop = mutation.target.style.top.substring(0, mutation.target.style.top.length - 2);
                customStyle.style.top = (newtop * 1.2) + 'px';
                mutation.target.dataset.top = customStyle.style.top;
            }
            if (mutation.target.dataset.height !== mutation.target.style.height) {
                let newheight = mutation.target.style.height.substring(0, mutation.target.style.height.length - 2);
                customStyle.style.height = (newheight * 1.2) + 'px';
                mutation.target.dataset.height = customStyle.style.height;
            }
        }
    });
});

observer.observe(customStyle, {
    attributes: true,  //configure it to listen to attribute changes,
    attributeFilter: ['style']
});
document.querySelector(".explorer-folders-view .monaco-list-rows").style.zoom = 1.2;
document.querySelector(".explorer-folders-view .monaco-list-rows").style.fontSize = '16px';