// endpoint /api/search/platform
import searchPlatformModel from '../../models/search/search.platform.model';
import searchPlatformHelper from '../../helpers/search/search.platform.helper';

const get = (req, res) => {
  console.log('inside searchPlatformController req.query is: ', req.query);
    return searchPlatformHelper.read(req.query)
    .then((platform) => {
      console.log('inside platform controller platform is: ', platform);
      if (platform.length) {
        res.status(200).send(platform);
      }
      res.status(500).end('no search results, check your parameters and try again.');
    })
    .catch((error) => {
      if (error) {
        console.log('error with that search: ', error);
        res.status(500).end(error);
      }
    });
};


exports.search = {
  get,
};