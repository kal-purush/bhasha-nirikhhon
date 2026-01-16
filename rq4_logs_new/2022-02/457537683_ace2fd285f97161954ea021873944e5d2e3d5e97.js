import "./assets/css/layout.css";
import { ThemeProvider } from "@mui/material";
import theme from "./theme/theme";
import { Provider } from "react-redux";
import store from "./store/store";
import React from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { PersistGate } from "redux-persist/integration/react";
import { persistStore } from "redux-persist";
import InterceptorsComponent from "./services/interceptorsComponent";
import ContentLoader from "./wigdets/ContentLoader";
import SearchGiphyPage from "./components/pages/SearchGiphyPage";
import NavBar from "./components/NavBar";
import FavoritesGiphyPage from "./components/pages/FavoritesGiphyPage";

function App() {
  let persistor = persistStore(store);
  return (
    <ThemeProvider theme={theme}>
      <Provider store={store}>
        <PersistGate loading={<ContentLoader />} persistor={persistor}>
          <InterceptorsComponent />
          <BrowserRouter>
            <NavBar />
            <Routes>
              <Route path="/" element={<SearchGiphyPage />} />
              <Route path="/favorites" element={<FavoritesGiphyPage />} />
            </Routes>
          </BrowserRouter>
        </PersistGate>
      </Provider>
    </ThemeProvider>
  );
}

export default App;