const RequestHelper = require('../helpers/request_helper.js');
const PubSub = require('../helpers/pub_sub.js');

const Search = function() {
  this.searchString = null;
};

Search.prototype.bindEvents = function() {
  PubSub.subscribe('SearchBoxView:SearchStringReady', event => {
    this.searchString = event.detail; 
    this.performSearch();
  });
};

Search.prototype.performSearch = function() {
  const url = new URL('https://esi.evetech.net/latest/search/');
  url.searchParams.append('search', this.searchString);
  url.searchParams.append('categories', 'character');

  const request = new RequestHelper(url);
  const searchPromise = request.get();
  searchPromise.then( data => {
    PubSub.publish('Search:GotSearchResults', data.character)  
  });
};


module.exports = Search;

