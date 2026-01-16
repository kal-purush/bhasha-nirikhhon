import { Results } from 'groupby-api';
import { Dispatch } from 'redux';
import { QueryTimeAutocompleteConfig, QueryTimeProductSearchConfig } from 'sayt';
import { Action, Adapters, Selectors, Store } from '..';
import FluxCapacitor from '../../flux-capacitor';
import { conditional, thunk } from '../utils';

export default class Creator {

  linkMapper: (value: string) => { value: string, url: string };

  constructor(private flux: FluxCapacitor, paths: Action.Paths) {
    this.linkMapper = (value: string) => ({ value, url: `${paths.search}/${value}` });
  }

  soFetching = (requestType: keyof Store.IsFetching) =>
    ({ type: Action.types.SO_FETCHING, requestType })

  // fetch action creators
  fetchMoreRefinements = (navigationId: string) =>
    (dispatch: Dispatch<any>, getStore: () => Store.State) => {
      const state = getStore();
      if (Selectors.hasMoreRefinements(state, navigationId)) {
        dispatch(this.soFetching('moreRefinements'));
        return this.flux.clients.bridge.refinements(Selectors.searchRequest(state), navigationId)
          .then(({ navigation: { name, refinements } }) => {
            const remapped = refinements.map(Adapters.Search.extractRefinement);
            return dispatch(this.receiveMoreRefinements(name, remapped));
          });
      }
    }

  fetchProducts = () =>
    (dispatch: Dispatch<any>, getStore: () => Store.State) => {
      dispatch(this.soFetching('search'));
      return this.flux.clients.bridge.search(Selectors.searchRequest(getStore()))
        .then((res) => dispatch(this.receiveSearchResponse(res)));
    }

  fetchAutocompleteSuggestions = (query: string, config: QueryTimeAutocompleteConfig) =>
    (dispatch: Dispatch<any>) => {
      dispatch(this.soFetching('autocompleteSuggestions'));
      return this.flux.clients.sayt.autocomplete(query, config)
        .then((res) => {
          const { suggestions, categoryValues } = Adapters.Search.extractAutocompleteSuggestions(res);
          dispatch(this.receiveAutocompleteSuggestions(suggestions, categoryValues));
        });
    }

  fetchAutocompleteProducts = (query: string, config: QueryTimeProductSearchConfig) =>
    (dispatch: Dispatch<any>) => {
      dispatch(this.soFetching('autocompleteProducts'));
      return this.flux.clients.sayt.productSearch(query, config)
        .then((res) => {
          const products = Adapters.Search.extractAutocompleteProducts(res);
          dispatch(this.receiveAutocompleteProducts(products));
        });
    }

  fetchCollectionCount = (collection: string) => (dispatch: Dispatch<any>, getStore: () => Store.State) =>
    this.flux.clients.bridge.search({ ...Selectors.searchRequest(getStore()), collection })
      .then((res) => dispatch(this.receiveCollectionCount(collection, res.totalRecordCount)))

  // request action creators
  updateSearch = (search: Action.Search) =>
    thunk(Action.types.UPDATE_SEARCH, Object.assign(search))

  selectRefinement = (navigationId: string, index: number) =>
    conditional<Action.Navigation.SelectRefinement>((state) =>
      Selectors.isRefinementDeselected(state, navigationId, index),
      Action.types.SELECT_REFINEMENT, { navigationId, index })

  deselectRefinement = (navigationId: string, index: number) =>
    conditional<Action.Navigation.DeselectRefinement>((state) =>
      Selectors.isRefinementSelected(state, navigationId, index),
      Action.types.DESELECT_REFINEMENT, { navigationId, index })

  selectCollection = (id: string) =>
    conditional<Action.Collections.SelectCollection>((state) =>
      state.data.collections.selected !== id,
      Action.types.SELECT_COLLECTION, { id })

  selectSort = (id: string) =>
    conditional<Action.Sort.UpdateSelected>((state) =>
      state.data.sorts.selected !== id,
      Action.types.SELECT_SORT, { id })

  updatePageSize = (size: number) =>
    conditional<Action.Page.UpdateSize>((state) =>
      state.data.page.size !== size,
      Action.types.UPDATE_PAGE_SIZE, { size })

  updateCurrentPage = (page: number) =>
    conditional<Action.Page.UpdateCurrent>((state) =>
      state.data.page.current !== page,
      Action.types.UPDATE_CURRENT_PAGE, { page })

  updateDetailsId = (id: string) =>
    thunk<Action.Details.UpdateId>(Action.types.UPDATE_DETAILS_ID, { id })

  updateAutocompleteQuery = (query: string) =>
    conditional<Action.Autocomplete.UpdateQuery>((state) =>
      state.data.autocomplete.query !== query,
      Action.types.UPDATE_AUTOCOMPLETE_QUERY, { query })

  // response action creators
  receiveSearchResponse = (results: Results) =>
    (dispatch: Dispatch<any>, getStore: () => Store.State) => {
      const state = getStore();
      dispatch(this.receiveRedirect(results.redirect));
      dispatch(this.receiveQuery(Adapters.Search.extractQuery(results, this.linkMapper)));
      dispatch(this.receiveProducts(results.records.map(Adapters.Search.extractProduct)));
      // tslint:disable-next-line max-line-length
      dispatch(this.receiveNavigations(Adapters.Search.combineNavigations(results.availableNavigation, results.selectedNavigation)));
      dispatch(this.receiveRecordCount(results.totalRecordCount));
      dispatch(this.receiveCollectionCount(state.data.collections.selected, results.totalRecordCount));
      dispatch(this.receivePage(Adapters.Search.extractPage(state)));
      dispatch(this.receiveTemplate(Adapters.Search.extractTemplate(results.template)));
    }

  receiveQuery = (query: Action.Query) =>
    thunk<Action.Query.ReceiveQuery>(Action.types.RECEIVE_QUERY, query)

  receiveProducts = (products: Store.Product[]) =>
    thunk(Action.types.RECEIVE_PRODUCTS, { products })

  receiveCollectionCount = (collection: string, count: number) =>
    thunk<Action.Collections.ReceiveCount>(
      Action.types.RECEIVE_COLLECTION_COUNT, { collection, count })

  receiveNavigations = (navigations: Store.Navigation[]) =>
    thunk<Action.Navigation.ReceiveNavigations>(
      Action.types.RECEIVE_NAVIGATIONS, { navigations })

  receivePage = (page: Action.Page) =>
    thunk<Action.Page.ReceivePage>(
      Action.types.RECEIVE_PAGE, page)

  receiveTemplate = (template: Store.Template) =>
    thunk(Action.types.RECEIVE_TEMPLATE, { template })

  receiveRecordCount = (recordCount: number) =>
    thunk(Action.types.RECEIVE_RECORD_COUNT, { recordCount })

  receiveRedirect = (redirect: string) =>
    thunk(Action.types.RECEIVE_REDIRECT, { redirect })

  receiveMoreRefinements = (navigationId: string, refinements: any) =>
    thunk(Action.types.RECEIVE_MORE_REFINEMENTS, { navigationId, refinements })

  receiveAutocompleteSuggestions = (suggestions: string[], categoryValues: string[]) =>
    thunk(Action.types.RECEIVE_AUTOCOMPLETE_SUGGESTIONS, { suggestions, categoryValues })

  receiveAutocompleteProducts = (products: Store.Product[]) =>
    thunk(Action.types.RECEIVE_AUTOCOMPLETE_PRODUCTS, { products })

  receiveDetailsProduct = (product: Store.Product) =>
    thunk<Action.Details.ReceiveProduct>(Action.types.RECEIVE_DETAILS_PRODUCT, { product })
}