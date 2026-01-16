#!/usr/bin/env node

// Why I don't use semi-colons in JavaScript: http://mislav.uniqpath.com/2010/05/semicolons/

var yargs = require('yargs')
var util = require('util')
var request = require('request-promise')
var Promise = require('bluebird')
var cheerio = require('cheerio')

var JSON_INDENT_SPACES = 2 // formatting of output

var BASE_URL = 'http://www.seetickets.com'
var SEARCH_URL = BASE_URL + '/search'
// Attempting to change the pageSize query string leads to changes to the text indicating
// the desired number of results but the list of events always contains 30 elements
var RESULTS_PER_PAGE = 30

// whisper is used for logging status updates when --verbose is used, it writes output to
// stderr so that stdout can be used for parsing the json generated from the page
var verbose
function whisper() {
  if (verbose)
    console.warn(util.format.apply(this, arguments))
}

function main() {
  var argv = require('yargs')
    .usage('Usage: $0 [-v] [-h] [-t type]')
    .help('h')
    .alias('h', 'help')
    .count('verbose')
    .describe('v', 'Increase verbosity')
    .alias('v', 'verbose')
    .default('t', 'comedy')
    .alias('t', 'type')
    .describe('t', 'Event type to retrieve')
    .argv

  verbose = argv.verbose
  printAllEvents(argv.type, 1)
  .then(function(result) {
    whisper('parsed all events')
  })
  .catch(function(err) {
    console.warn('Issue parsing events %s', err.stack ? err.stack : err)
  })
}

/**
 * Print all pages on after the other.
 * @returns {Promise} Resolves when all events on all pages have been parsed.
 */
function printAllEvents(type, currentPage) {
  return parseEventsOnPage(type, currentPage)
  .then(function(eventsOnPage) {
    if (eventsOnPage.length) {
      // this strategy streams json after each page has been parsed, if I were
      // to use FRP (bacon.js/rx) then a higher granularity of streaming could be
      // achieved (with greater concurrency) but I went for an approach that
      // won't scare off those not used to functional programming.
      eventsOnPage.forEach(function(event) {
        console.log(JSON.stringify(event, null, JSON_INDENT_SPACES))
      })

      // if the previous page had results then extend the promise with results from
      // the next page
      return printAllEvents(type, currentPage + 1)
    }
  })
}

/**
 * Parse events for given event type and page.
 * @return {Promise} Resolves when all event data for the page has been retrieved.
 */
function parseEventsOnPage(type, pageIndex) {
  whisper('parsing %s events on page %s', type, pageIndex)

  return request.get({
    url: SEARCH_URL,
    qs: { s: type, c: 76, q: '', pageIndex: pageIndex }
  })
  .then(function(body) {
    if (body.indexOf('There were no results for your search.') !== -1) {
      // no more results left
      return []
    }

    var $ = cheerio.load(body)
    var eventPromises = $('article').map(function(idx, article) {
      return parseArticle(cheerio(article))
    }).get()

    whisper('%s events on page %s', eventPromises.length, pageIndex)
    return Promise.all(eventPromises)
  })
}


var venueAddresses = {} // cache of venue address data by relative url to venue

/**
 * Parse the given article, returning a promise as an additional request may have to be
 * made to retrieve the venue address data.
 * @return {Promise} Resolves with given event data.
 */
function parseArticle(article$) {
  var eventLink$ = article$.find('a').eq(0)

  var event = {
    name: eventLink$.text(),
    event_link: BASE_URL + eventLink$.attr('href'),
  }

  // some events span multiple dates and don't list a specific date field
  var date$ = article$.find('.date-wrap')
  if (date$.length) {
    event.date = [
      date$.find('.weekday').text(),
      date$.find('.day').text(),
      date$.find('p').text(),
      article$.find('.result-text >p').last().text()
    ].join(' ')
  }

  var artists = article$.find('.supports a').map(function(idx, el) {
    return cheerio(el).text()
  }).get()
  if (artists.length)
    event.artists = artists

  var venueLink$ = article$.find('p a')
  if (venueLink$.length === 0) {
    // not everything has a venue... e.g. glasgow international comedy venue lists "many"
    return event
  }

  var venueUrl = BASE_URL + venueLink$.attr('href')
  event.venue_name = venueLink$.text()
  event.venue_link = venueUrl

  if (venueUrl in venueAddresses) {
    event.venue_address = venueAddresses[venueUrl]
    return event
  }
  else {
    whisper('requesting venue data for %s: %s', event.name, venueUrl)

    return request.get(venueUrl).then(function(venueBody) {
      event.venue_address = 'poopy'
      var $ = cheerio.load(venueBody)

      var address = $('.location').text().trim()
      // not every venue has a listed address
      if (address)
        address = address.split(/\s*,\s*/)
      // add cache entry even when there is no address to avoid duplicate requests
      venueAddresses[venueUrl] = address
      if (address !== '')
        event.venue_address = address

      return event
    })
  }
}

main() // live long and prosper