import React from "react";
import { createStackNavigator } from "react-navigation";

import Login from "./src/components/Login";
import Register from "./src/components/Register";
import ForgotPassword from "./src/components/ForgotPassword";
import Todo from "./src/components/Todo";

const MainNav = createStackNavigator(
    {
        Login: Login,
        Register: Register,
        ForgotPassword: ForgotPassword,
        Todo: Todo
    },
    {
        navigationOptions: {
            header: null
        }
    }
);

const App = () => <MainNav />;

export default App;