import React from 'react';
import ApolloClient from 'apollo-boost';
import { ApolloProvider } from 'react-apollo'; // bind apollo to react
import './App.css';
import './black-dash.css';

// components
import BooksList from './components/BooksList';
import AuthorsList from './components/AuthorsList';

// Apollo client setup
const client =  new ApolloClient({
  uri: 'http://localhost:3400/graphql'  // the end point where we will be making requests to
});

function App() {
  return (
    <ApolloProvider client={ client }> {/* wrap our app and inject whatever data we receive from server into our app */}
      <div className="App container">
        <header className="">
        </header>
        <BooksList/>
        <AuthorsList/>
      </div>
    </ApolloProvider>
  );
}

export default App;