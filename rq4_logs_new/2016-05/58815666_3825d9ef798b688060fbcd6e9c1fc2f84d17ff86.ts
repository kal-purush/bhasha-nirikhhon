const route = (option) => {
  const start = option.start;
  const stop = option.stop;
  const log = option.log;
  const other = option.other;
  return (req, res, next) => {
    switch (req.url) {
      case '/suty/log': {
        log(req, res, next);
        return;
      }
      case '/suty/start': {
        start(req, res, next);
        return;
      }
      case '/suty/stop': {
        stop(req, res, next);
        return;
      }
    }
    other();
    next();
  }
};

export default route;