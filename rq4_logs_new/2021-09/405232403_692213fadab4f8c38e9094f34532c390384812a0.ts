// package imports
import axios, { AxiosError } from "axios";
import { Dispatch } from "redux";
// non- package imports
import { clearErrors } from "./errorActions";
import { actionShowLoader } from "./loaderActions";
import { errorHandler } from "helpers";

// List of action types to be used
//TODO:  check what I really need to keep
import { ActionTypes } from "./types";

// Retrieve wiki data
export const retrieveWikiSearchValues =
  (url: string, successCb?: Function) => (dispatch: Dispatch) => {
    let wikiSearchReturnValues: any = [];
    axios
      .get(url)
      // retrieve relevant properties, aggregate them into an object, and add them to the array
      .then((res: any) => {
        for (let key in res.data.query.search) {
          wikiSearchReturnValues.push({
            queryResultPageFullURL: "no link",
            queryResultPageID: res.data.query.search[key].pageid,
            queryResultPageTitle: res.data.query.search[key].title,
            queryResultPageSnippet: res.data.query.search[key].snippet,
          });
        }
      })
      // update the empty query result page full URL by performing another GET request using the previously retrieved ID
      .then(() => {
        for (let key2 in wikiSearchReturnValues) {
          let page = wikiSearchReturnValues[key2];
          let pageID = page.queryResultPageID;
          let urlForRetrievingPageURLByPageID = `https://en.wikipedia.org/w/api.php?origin=*&action=query&prop=info&pageids=${pageID}&inprop=url&format=json`;
          axios.get(urlForRetrievingPageURLByPageID).then((res: any) => {
            page.queryResultPageFullURL = res.data.query.pages[pageID].fullurl;
          });
        }
      })
      .then(() => {
        dispatch({
          type: ActionTypes.ADD_WIKI_SEARCH_RETURN_VALUES_SUCCESS,
          payload: wikiSearchReturnValues,
        });
        dispatch(clearErrors());
        if (successCb) successCb();
      })
      // handle failure by logging and displaying a visual indicator
      .catch((err: AxiosError) => {
        errorHandler(dispatch, err, {
          type: ActionTypes.ADD_WIKI_SEARCH_RETURN_VALUES_FAIL,
        });
      })
      // remove the loader
      .finally((): void => {
        dispatch(actionShowLoader("wikiSearch", false));
      });
  };