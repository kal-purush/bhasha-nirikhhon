import headerView from './header_view.js';

export default function loadingPostsView(store) {
  let state = store.getState();

  //Create the HTML
  let $viewHtml = $(` <section class="page-wrapper chat-view"></section>`);

  $viewHtml.prepend(headerView(store));

  let $contentWrapper = $('<div class="view-content">');
  $viewHtml.append($contentWrapper);

  let $postsContent = $(` <h2>loading posts…</h2>`);
  $contentWrapper.append($postsContent);



  //Assign any event listeners
  $($viewHtml).find('h2').on('click', () => {
    store.dispatch(exampledsAsyncAction());
  });

  // return html of view
  return $viewHtml;
}