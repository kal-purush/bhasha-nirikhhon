import express from 'express';
import bodyParser from 'body-parser';
import { filterImageFromURL, deleteLocalFiles } from './util/util';

(async () => {

  // Init the Express application
  const app = express();
  let images:string[] = [];
  // Set the network port
  const port = process.env.PORT || 8082;

  // Use the body parser middleware for post requests
  app.use(bodyParser.json());

  // Root Endpoint
  // Displays a simple message to the user
  app.get("/filteredimage", async (req, res) => {

    const { image_url } = req.query;

    // validating the image_url present in request's query
    if (!image_url) {
      return res.status(400).send({ message: 'image not found' })
    }
    deleteLocalFiles(images);

    // calling filterImageFromURL(image_url) to filter the image
    const filteredpath = await filterImageFromURL(image_url);
    images.push(filteredpath);

    if (!filteredpath) {
      return res.status(422).send({ message: 'unable to process image for filtering due to internal errors' })
    }
    
    res.status(200).sendFile(filteredpath);   

  });


  // Root URI call
  app.get("/", async (req, res) => {
    res.send("Try /filteredimage?image_url={{PUBLIC URL}}");
  });

  // Start the Server
  app.listen(port, () => {
    console.log(`server running http://localhost:${port}`);
    console.log(`press CTRL+C to stop server`);
  });
})();