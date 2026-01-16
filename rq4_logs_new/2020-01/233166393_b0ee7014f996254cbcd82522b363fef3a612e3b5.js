const express = require("express");
const next = require("next");
const promMid = require("express-prometheus-middleware");
const port = parseInt(process.env.PORT, 10) || 3000;
const dev = process.env.NODE_ENV !== "production";
const app = next({
  dev
});
const handle = app.getRequestHandler();
const server = express();

app.prepare().then(() => {
  server.use(
    promMid({
      metricsPath: "/metrics",
      collectDefaultMetrics: true,
      requestDurationBuckets: [0.1, 0.5, 1, 1.5]
    })
  );
  server.listen(port, err => {
    if (err) throw err;
    console.log(`==> Сервер запущен на http://localhost:${port}`);
  });

  server.get("*", (req, res) => {
    return handle(req, res);
  });
});