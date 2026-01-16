const terminal = (msg) => {
    if ((process.env.NODE_ENV == 'development') && (msg != '')) {
        console.log(msg);
    }
};

export default terminal;