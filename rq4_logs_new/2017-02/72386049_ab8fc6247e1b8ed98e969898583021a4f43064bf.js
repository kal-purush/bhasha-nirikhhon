export function gallery(gallerySelector = '.gallery', overlaySelector = '.gallery-overlay') {
  const $ = window.jQuery || {};

  const $gallery = $(gallerySelector);

  const cachedGallery = {};

  if (!$gallery.length) {
    return false;
  }

  const $overlay = $(overlaySelector);

  $overlay.on('click', (e) => {
    e.preventDefault();
    if (!$(e.target).is('img')) {
      $overlay.css({ display: 'none' });
    }
  });

  function appendImg(path) {
    const $img = $('<img>').attr('src', path);

    $overlay.find('.gallery-image')
    .empty()
    .append($img);

    $overlay.css({ display: 'block' })
    .focus();
  }

  $gallery.find('a').each((index, item) => {
    const $link = $(item);
    const url = $link.attr('href');
    if (!url) {
      return true;
    }

    $link.on('click', (e) => {
      e.preventDefault();

      if (cachedGallery[url]) {
        appendImg(cachedGallery[url]);
        return true;
      }

      $.ajax({
        url,
        type: 'GET',
        success: (response) => {
          const imgPath = $(response).find('.entry-attachment img').attr('src');
          cachedGallery[url] = imgPath;
          appendImg(imgPath);
        }
      });
      return true;
    });

    return true;
  });

  return true;
}

export default gallery;