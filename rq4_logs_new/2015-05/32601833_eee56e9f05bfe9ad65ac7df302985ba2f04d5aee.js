'use strict';

var Q = require('q');

var searchCache = require('./searchInterfaceCache');
var searchInterface = require('gramene-search-client').client;

function checkRequestedResultTypesArePresent(data) {
  _.forIn(data.metadata.searchQuery.resultTypes,
    function (params, key) {
      if (!data[key]) {
        console.error(key + ' not found in search results');
      }
    });

  return data;
}

module.exports = {
  // Note that this function is debounced in init. It might be called many
  // times in succession when a user is interacting with the page,
  // but only the last one will fire.
  search: function (query, searchComplete, searchError) {
    console.log('performing search', query);

    // make a copy of the query state when we make the async call...
    var queryCopy = _.cloneDeep(query);

    // find cached results and move them from
    // query.resultTypes to query.cachedResultTypes
    searchCache.findCachedResults(queryCopy);

    // if we have any uncached result types, we need to
    // ask the server for some data, otherwise we will
    // just use what we have
    var promise;
    if(_.size(queryCopy.resultTypes) || !queryCopy.count) {
      promise = this.searchPromise(queryCopy)

        // when we get data from the server, put it in the
        // cache
        .then(searchCache.addResultsToCache(queryCopy))

        // and also add the query for the actual search
        // to the results metadata.
        .then(function addQueryToResults(data) {
          data.metadata.searchQuery = queryCopy;
          return data;
        });
    }
    else {
      promise = this.nullSearchPromise(queryCopy);
    }

    promise
      // add any cached data
      .then(searchCache.getResultsFromCache)

      // console.log for anything we asked for but didn't get
      // TODO should we error out here?
      .then(checkRequestedResultTypesArePresent)

      // tell interested parties about what has happened
      .then(searchComplete)
      .catch(searchError);
  },

  searchPromise: function(query) {
    console.log('asking search interface for', query);
    return searchInterface.geneSearch(query);
  },

  nullSearchPromise: function(query) {
    return Q.fcall(function refactorQueryToHaveShapeOfResponse() {
      console.log('remote query not required');
      var metadata = {searchQuery: query, count: query.count};
      return {metadata: metadata};
    });
  }
};