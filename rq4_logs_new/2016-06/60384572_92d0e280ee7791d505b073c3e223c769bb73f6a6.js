module.exports = function Route(app){
  app.get('/', function(request, respond){
    respond.render("index");
  })

  app.post('/submit', function(req,res){
    // console.log('hello fucker');
  })

}