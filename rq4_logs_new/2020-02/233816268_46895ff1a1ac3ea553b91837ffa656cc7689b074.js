function createIframe (elClass, params) {
    if ($("div." + elClass).find('iframe').length) {
        return;
    }

    const iframeEl = document.createElement('iframe');
    iframeEl.src = params.src;
    iframeEl.title = params.title;
    iframeEl.allow = params.allow;
    iframeEl.style = params.style;
    iframeEl.sandbox = params.sandbox;
    $("div." + elClass).append(iframeEl);
    $("div." + elClass)
        .parent()
        .siblings()
        .find('iframe')
        .remove();
}
function createFirstIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/clever-cerf-mphb1?fontsize=14&hidenavigation=1&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }

    createIframe('iframe', iframeData);
}
function createSecondIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/csstask-qf18c?fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe2', iframeData);
}







function createThirdIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/flamboyant-rubin-txopb?expanddevtools=1&fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe3', iframeData);
}
function createFourthIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/flamboyant-rubin-txopb?expanddevtools=1&fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe4', iframeData);
}
function createFifthIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/flamboyant-rubin-txopb?expanddevtools=1&fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe5', iframeData);
}
function createSixthIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/flamboyant-rubin-txopb?expanddevtools=1&fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe6', iframeData);
}
function createSeventhIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/flamboyant-rubin-txopb?expanddevtools=1&fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe7', iframeData);
}
function createEightsIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/flamboyant-rubin-txopb?expanddevtools=1&fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe8', iframeData);
}
function createNinethIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/flamboyant-rubin-txopb?expanddevtools=1&fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe9', iframeData);
}
function createTenthIframe () {
    const iframeData = {
        src: "https://codesandbox.io/embed/flamboyant-rubin-txopb?expanddevtools=1&fontsize=14&hidenavigation=1&module=%2Findex.html&theme=dark",
        title: "flamboyant-rubin-txopb",
        allow: "geolocation; microphone; camera; midi; vr; accelerometer; gyroscope; payment; ambient-light-sensor; encrypted-media; usb",
        style:"width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;",
        sandbox:"allow-modals allow-forms allow-popups allow-scripts allow-same-origin",
    }
    createIframe('iframe10', iframeData);
}



