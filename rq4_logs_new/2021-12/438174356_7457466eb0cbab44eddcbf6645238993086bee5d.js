import React, { Component } from "react";
import ImageGallery from "./components/ImageGallery/ImageGallery";
import Modal from "./components/Modal/Modal";
import Searchbar from "./components/Searchbar/Searchbar";
import Button from "./components/Button/Button";
import AppLoader from "./components/Loader/Loader";
import fetchImages from "./services/pixabayApi";

class App extends Component {
  state = {
    hits: [],
    currentPage: 1,
    searchQuery: "",
    largeImageURL: "",
    isLoading: false,
    scrollHeight: 0,
  };

  componentDidUpdate(prevProps, prevState) {
    if (prevState.searchQuery !== this.state.searchQuery) {
      this.fetchHits();
    }
  }

  onChangeQuery = (query) => {
    this.setState({
      searchQuery: query,
      currentPage: 1,
      hits: [],
    });
  };

  fetchHits = () => {
    const { currentPage, searchQuery, scrollHeight } = this.state;

    const options = {
      searchQuery,
      currentPage,
    };

    const galleryLink = document.getElementById("gallery");

    this.setState({ isLoading: true });

    fetchImages(options)
      .then((hits) => {
        this.setState((prevState) => ({
          hits: [...prevState.hits, ...hits],
          currentPage: prevState.currentPage + 1,
        }));
      })
      .catch((error) => console.log(error))
      .finally(() => {
        this.setState({ isLoading: false });
        window.scrollTo({
          top: scrollHeight,
          behavior: "smooth",
        });

        this.setState({ scrollHeight: galleryLink.scrollHeight + 18 });
      });
  };

  toggleModal = (url) => {
    this.setState({ largeImageURL: url ? url : "" });
  };

  render() {
    const { hits, isLoading, largeImageURL } = this.state;
    return (
      <div className="App">
        <Searchbar onSubmit={this.onChangeQuery} />
        {largeImageURL && (
          <Modal onClose={this.toggleModal} url={largeImageURL} />
        )}
        <ImageGallery hits={hits} onClick={this.toggleModal} />
        {hits.length > 0 && !isLoading && <Button fetchHits={this.fetchHits} />}
        {isLoading && <AppLoader />}
      </div>
    );
  }
}

export default App;