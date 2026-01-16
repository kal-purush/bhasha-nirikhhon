import React from "react"
import { createStackNavigator } from "@react-navigation/stack"
import ListingScreen from "../screens/ListingScreen";
import ListingDetails from "../components/ListingDetails"
import AccountScreen from "../screens/AccountScreen";
import Messages from "../components/Messages"

const Stack = createStackNavigator();

const AccountNavigator = () => (
    <Stack.Navigator headerMode="none" mode="modal">
        <Stack.Screen name="Account" component={AccountScreen} />
        <Stack.Screen name="Messages" component={Messages} />
    </Stack.Navigator>
)

export default AccountNavigator;