import { PaginationUrlParamsModel } from './../../../models/pagination-url-params.model';


export function proccessReducer(
  action: any,
  typeActions: any,
  state: any
) {
  switch (action.type) {
    case (typeActions.SET_DATA):
      return {
        ...state,
        tableData: action.payload
      }
    case (typeActions.SET_TOTAL_RECORDS):
      const urlParamsTotalRecords = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsTotalRecords.setTotalRecords(action.payload);

      return {
        ...state,
        urlParams: urlParamsTotalRecords
      };
    case (typeActions.NEXT_PAGE):
      const urlParamsNextPage = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsNextPage.nextPage();

      return {
        ...state,
        urlParams: urlParamsNextPage
      }
    case (typeActions.PREVIOUS_PAGE):
      const urlParamsPreviousPage = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsPreviousPage.previousPage();

      return {
        ...state,
        urlParams: urlParamsPreviousPage
      }
    case (typeActions.FIRST_PAGE):
      const urlParamsFirstPage = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsFirstPage.firstPage();

      return {
        ...state,
        urlParams: urlParamsFirstPage
      }
    case (typeActions.LAST_PAGE):
      const urlParamsLastPage = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsLastPage.lastPage();

      return {
        ...state,
        urlParams: urlParamsLastPage
      }
    case (typeActions.RESET_PAGINATION):
      const urlParamsResetPagination = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsResetPagination.resetPagination();
      return {
        ...state,
      }
    case (typeActions.SET_PAGE_SIZE):
      const urlParamsSetRecordsPerPage = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsSetRecordsPerPage.setPageSize(action.payload);

      return {
        ...state,
        urlParams: urlParamsSetRecordsPerPage
      }
    case (typeActions.SET_SORT):
      const urlParamsSetSort = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsSetSort.setSort(action.payload);

      return {
        ...state,
        urlParams: urlParamsSetSort
      }

    case (typeActions.SET_FILTER):
      const urlParamsSetFilter = new PaginationUrlParamsModel({...state.urlParams});
      urlParamsSetFilter.setFilter(action.payload);

      return {
        ...state,
        urlParams: urlParamsSetFilter
      }

    case (typeActions.SET_SELECTED_ROW):
      return {
        ...state,
        selectedRow: action.payload
      }

    case (typeActions.SET_FORM_ITEM):
      return {
        ...state,
        activeFormItem: action.payload
      }

    case (typeActions.DELETE_FORM):
      return {
        ...state,
        selectedRow: null
      }

    default:
      return state;
  }
}