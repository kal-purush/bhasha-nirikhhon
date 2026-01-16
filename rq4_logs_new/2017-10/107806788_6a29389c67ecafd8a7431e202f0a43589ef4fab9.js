console.log("API routes loaded...")

// API ROUTES
app.post("/api/tables", function(req, res){
  var tableReady = reservations.length < 6 ? true : false;
  reservations.push(req.body);
  res.json(tableReady);
})

app.get("/api/tables", function(req, res){
  res.json(reservations.slice(0, 5));
})

app.get("/api/waitlist", function(req, res){
  res.json(reservations.slice(5));
})

exports.app = apiRoutes;