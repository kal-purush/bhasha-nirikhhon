import express from 'express';
import bodyParser from 'body-parser';
import {filterImageFromURL, deleteLocalFiles} from './util/util';

(async () => {

  // Init the Express application
  const app = express();

  // Set the network port
  const port = process.env.PORT || 8082;
  
  // Use the body parser middleware for post requests
  app.use(bodyParser.json());

  // @TODO1 IMPLEMENT A RESTFUL ENDPOINT
  // GET /filteredimage?image_url={{URL}}
  // endpoint to filter an image from a public url.
  app.get("/filteredimage", async(req, res) => {
    let imageUrl = req.query.image_url;
    // 1. validate the image_url query
    if(imageUrl != null){     
    // 2. call filterImageFromURL(image_url) to filter the image
      let result = await filterImageFromURL(imageUrl);
    // 3. send the resulting file in the response
      res.sendFile(result, (err) => {
        // 4. deletes any files on the server on finish of the response
        deleteLocalFiles([result]);
      });
    }else{
      res.send({status : "image_url is required"});
    }
  });
  
  // Root Endpoint
  // Displays a simple message to the user
  app.get( "/", async ( req, res ) => {
    res.send(`
      <h1>Udagram API</h1>
      <p>Welcome to Udagram ! </p>
      <hr>
      <small>Version: 1.0.0 </small>
      <small>Author : Nehemie KOFFI </small>`
    )
  });
  

  // Start the Server
  app.listen( port, () => {
      console.log( `server running http://localhost:${ port }` );
      console.log( `press CTRL+C to stop server` );
  } );
})();