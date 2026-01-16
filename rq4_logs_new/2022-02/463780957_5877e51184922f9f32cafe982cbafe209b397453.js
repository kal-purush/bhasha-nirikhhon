require("./bootstrap");
// import "../sass/app.scss";
import React from "react";
import { render } from "react-dom";
import { BrowserRouter } from "react-router-dom";
import ErrorBoundry from "./components/error-boundry";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import { ruRU } from "@mui/material/locale";
import Body from "./components/body";
import { Provider } from "react-redux";
import store from "./redux/store";
import { ServiceProvider } from "./services/context";
import RequestService from "./services/request";

const theme = createTheme(
    {
        palette: {
            primary: { main: "#1976d2" },
        },
    },
    ruRU
);

const requestService = new RequestService();

if (module.hot) module.hot.accept();

console.log(document)

render(
    <Provider store={store}>
        <ErrorBoundry>
            <ServiceProvider value={requestService}>
                <BrowserRouter>
                    <ThemeProvider theme={theme}>
                        <Body />
                    </ThemeProvider>
                </BrowserRouter>
            </ServiceProvider>
        </ErrorBoundry>
    </Provider>,
    document.getElementById("root")
);