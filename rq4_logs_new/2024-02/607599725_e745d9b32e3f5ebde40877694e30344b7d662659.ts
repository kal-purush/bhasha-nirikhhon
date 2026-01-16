export class PageModel {
  /// <summary>
  /// Page Size
  /// </summary>
  page_size: number;

  /// <summary>
  /// Current Page
  /// </summary>
  current_page: number;

  /// <summary>
  /// Total Number of Items
  /// </summary>
  total_count: number;

  /// <summary>
  /// Specfies whether there is a previous page
  /// </summary>
  has_previous: boolean;

  /// <summary>
  /// Specfies whether there is a next page
  /// </summary>
  has_next: boolean;

  constructor(
    page_size: number,
    current_page: number,
    total_count: number,
    has_previous: boolean,
    has_next: boolean
  ) {
    this.page_size = page_size;
    this.current_page = current_page;
    this.total_count = total_count;
    this.has_previous = has_previous;
    this.has_next = has_next;
  }
}