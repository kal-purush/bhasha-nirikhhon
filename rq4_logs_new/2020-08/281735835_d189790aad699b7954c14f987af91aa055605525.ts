import {readUrls, readAlreadyAddedAutomationUrls} from './readUrls';
import {getNewUrlsFromManySources} from './getNewUrls';
import {writeURL} from './writeUrl';

const dedupe = async () => {
  const workingUrls = await readUrls();
  const writtenUrls = await readAlreadyAddedAutomationUrls();

  let urls: URL[] = [];
  writtenUrls.forEach((value) => {
    urls = [...urls, ...value];
  });

  const deduped = getNewUrlsFromManySources(urls, workingUrls);
  console.log(urls.length);
  console.log(deduped.length);

  await writeURL(deduped, 'Deduped');
};

export {dedupe};