import express, { Request, Response } from 'express';
import bodyParser from 'body-parser';
import { query, validationResult } from 'express-validator';
import {filterImageFromURL, deleteLocalFiles} from './util/util';


(async () => {
  // validate the image_url query
  const isURLOptions = {
    require_host: true,
    require_tld: true
  }

  const app = express();
  const port = process.env.PORT || 8080;
  app.use(bodyParser.json());

  // Root Endpoint
  app.get( "/", async ( req, res ) => {
    res.send("try GET /filteredimage?image_url={{}}")
  } );

  // GET /filteredimage?image_url={{URL}}
  app.get( 
    "/filteredimage/",
    [
      query('image_url').isURL(isURLOptions).withMessage('Should have a valid url'),
    ],
    async ( req: Request, res: Response ) => {
    
    // validate the image_url query
    const errors = validationResult(req);
    
    if (!errors.isEmpty()) {
      return res.status(400)
                .json({ errors: errors.array() });
    }

    // call filterImageFromURL(image_url) to filter the image
    const filteredpath = await filterImageFromURL(req.query)

    return res.sendFile(filteredpath, ()=>{
      deleteLocalFiles([filteredpath]) // deletes any files on the server on finish of the response
    })
      
  });

  // Start the Server
  app.listen( port, () => {
      console.log( `server running http://localhost:${ port }` );
      console.log( `press CTRL+C to stop server` );
  } );
})();