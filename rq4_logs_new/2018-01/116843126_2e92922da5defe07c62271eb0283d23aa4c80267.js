
import React, { Component } from 'react'
import { Link } from 'react-router-dom'
import Book from './Book'
import escapeRegExp from 'escape-string-regexp'
import sortBy from 'sort-by'
import PropTypes from 'prop-types'

class SearchBooks extends Component {
  static propTypes = {
    myBooks: PropTypes.array,
    searchBooks: PropTypes.array,
    updateMyBookRequest: PropTypes.func.isRequired,
    keyWordTrigger: PropTypes.func.isRequired
  }

  state = {
    query: ''
  }

  updateQuery = (query) => {
    this.setState({query: query})
    this.checkForKeywords(query)
  }

  checkForKeywords = (query) => {
    this.props.keyWordTrigger(query)
  }

  getShowingBooks = () => {
    let showingBooks
    if (this.state.query) {
        const match = new RegExp(escapeRegExp(this.state.query), 'i')
        showingBooks = this.props.searchBooks.filter((book) => match.test(book.title) || match.test(book.authors))
    } else {
        showingBooks = this.props.searchBooks
    }
    showingBooks.sort(sortBy('title'))
    return showingBooks
  }

  render() {

    let showingBooks = this.getShowingBooks()

    return (
      <div className="search-books">
        <div className="search-books-bar">
          <Link className="close-search" onClick={() => this.setState({ showSearchPage: false })} to="/">Close</Link>

          <div className="search-books-input-wrapper">
            <input
              type="text"
              placeholder="Search by title or author"
              value={this.state.query}
              onChange={(event) => this.updateQuery(event.target.value)}
            />
          </div>
        </div>

        <div className="search-books-results">
          <ol className="books-grid">
            {showingBooks.map((book, index) => (
                <Book
                  key={index}
                  book={book}
                  updateMyBookRequest={(book, shelf) => {
                    this.props.updateMyBookRequest(book, shelf)
                  }}
                />
              ))}
          </ol>
        </div>
      </div>
    )
  }
}

export default SearchBooks