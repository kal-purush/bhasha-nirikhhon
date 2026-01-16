const dispatch = (type, detail, customTarget) => {
    const target = customTarget || document;
    const event = new CustomEvent(type, { detail });

    target.dispatchEvent(event);
};


const addListener = (key, callback, customTarget) => {
    const target = customTarget || document;

    target.addEventListener(key, (event) => {
        callback(event.detail);
    });
};

module.exports = {
    dispatch,
    addListener
};