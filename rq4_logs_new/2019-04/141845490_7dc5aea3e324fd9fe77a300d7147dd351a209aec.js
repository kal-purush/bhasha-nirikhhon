import Do from "../../const/do"

export default {
    [Do.SET_SEARCH_TYPE]: (state, v) => state.search.type = v,
    [Do.SET_SEARCH_CATS]: (state, v) => state.search.cats = v,
    [Do.SET_SEARCH_TERM]: (state, v) => state.search.term = v,
    [Do.SET_SEARCH_OWNER]: (state, v) => state.search.owner = v,
    [Do.SET_SEARCH_IMPACT]: (state, v) => state.search.impact = v,
    [Do.SET_SEARCH_FACET]: (state, v) => state.search.facet = v
}