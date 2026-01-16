window.addEventListener('load', () => {
  // TODO: Some of this could use the intersection observer
  // api to load src on scroll.
  const elements = document.querySelectorAll('[data-src]');
  for (const e of elements) {
    e.setAttribute('src', e.getAttribute('data-src'));
  }

  const videos = document.querySelectorAll('video');
  for (const v of videos) {
    v.load();
  }
});