export const PaginationTemplate = (model) => `
<section class="pagination-container">
<section class="current-page-container">
  <p>Showing ${model.currentPage} out of ${model.numberOfPages} Total Pages</p>
</section>
<section class="pagination-buttons">
  <button id="first-page">First</button>
  <button id="next-page">Next</button>
  <button id="previous-page">Previous</button>
  <button id="last-page">Last</button>
</section>
</section>
`