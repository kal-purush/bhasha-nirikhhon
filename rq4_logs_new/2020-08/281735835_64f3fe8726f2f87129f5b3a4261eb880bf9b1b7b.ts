import {readUrls} from './readUrls';
import {searchSite} from './searchSite';
import {getNewUrlsFromManySources} from './getNewUrls';
import {writeURL} from './writeUrl';

const gatherUrls = async () => {
  const existingData: Map<string, URL[]> = await readUrls();
  const domains = [...existingData.keys()].slice(0, 1);
  let searchResults : URL[] = [];

  const searchPromises: Promise<void>[] = [];
  domains.forEach((domain) => {
    const searchTerm = `site:${domain} Chandigarh`;
    const searchPromise = searchSite(searchTerm, 10).then((urls) =>{
      searchResults = [...searchResults, ...urls];
    });
    searchPromises.push(searchPromise);
  });

  Promise.all(searchPromises);

  const newUrls : URL[] = getNewUrlsFromManySources(searchResults, existingData);
  writeURL(newUrls);
};

export {gatherUrls};