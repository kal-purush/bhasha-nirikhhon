export default (data) => {
  const getTitle = node => node.querySelector('title').textContent;
  const getDescription = node => node.querySelector('description').textContent;
  const getLink = node => node.querySelector('link').textContent;

  const getNodeData = node => ({
    title: getTitle(node),
    description: getDescription(node),
    link: getLink(node),
  });

  const parser = new DOMParser();
  const doc = parser.parseFromString(data, 'text/xml');

  if (doc.firstChild.tagName !== 'rss') {
    return false;
  }

  const { title, description } = getNodeData(doc);
  const channelItems = [...doc.querySelectorAll('item')].map(getNodeData);

  return {
    title,
    description,
    channelItems,
  };
};