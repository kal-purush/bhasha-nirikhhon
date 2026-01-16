var Promise=require("promise");
var url=require("url");

export class BaseHandler
{
  response:any;
  request:any;
  query:any;

  constructor(){

  }
  public writeToJSON(data)
  {
    this.response.end(JSON.stringify(data));
  }
  public redirect(url)
  {
    this.response.writeHead(302,{'Location':url});
    this.response.end("redirect to "+url);
  }
  public receiveData(onSuccess,onError)
  {
    var body="";
    this.request.addListener('data',(chunk) =>
    {
      body += chunk;
      if(body.length > 1e6)
      {
        this.request.connection.destroy();
        onError("Out of memory");
      }
    });
    this.request.addListener("error",(error)=>
    {
      onError(error);
    });
    this.request.addListener("end",(chunk)=>
    {
      if(chunk)
      {
        body += chunk;
      }
      onSuccess(body);
    })
  }
  public getQuery()
  {
    if(!!!this.query)
    {
      this.query = url.parse(this.request.url,true).query;
    }
    return this.query;
  }
}