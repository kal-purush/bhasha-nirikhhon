import { Router, Request, Response } from 'express';
import { filterImageFromURL, validURL, deleteLocalFiles } from '../../../util/util';

const router: Router = Router();

  // > try it {{host}}/cars?filter=make
  router.get( "/filteredimage", async ( req: Request, res: Response ) => {
    let { image_url } = req.query;

    if (!image_url) {
        res.status(400).send(`Image URL is required`);
    }

    //validate the query URL
    if ( !validURL(image_url) ) {
        res.status(400).send('Malformed image URL');
    }

    const filtered_url = filterImageFromURL(image_url);

/*     filtered_url.catch(error => {
        console.error(error);
        res.status(400).send(error);
    }); */

    filtered_url.then(image => {
        res.status(200).sendFile(image, err => {
            if (err) {
                console.error(err);
            } else {
                deleteLocalFiles([image]);
            }
        });
    }).catch(error => {
        console.error(error);
        res.status(400).send(error);
    });

  } );


export const FilteredimageRouter: Router = router;