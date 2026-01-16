import React from "react";
import 'react-native-gesture-handler';
import { NavigationContainer } from "@react-navigation/native";
import { createStackNavigator } from "@react-navigation/stack";
import {createDrawerNavigator } from "@react-navigation/drawer";
import { createMaterialTopTabNavigator } from "@react-navigation/material-top-tabs";
import { createBottomTabNavigator } from "@react-navigation/bottom-tabs";
import { Ionicons } from '@expo/vector-icons';


import Login from "./src/Login";
import Signup from "./src/signup";
import Flex from "./src/Flex"; 
import Home from "./src/Drawer/Home";
import Profile from "./src/Drawer/Profile";
import Settings from "./src/Drawer/Settings";
import AboutUs from "./src/Drawer/AboutUs";
import Exit from "./src/Drawer/Exit";


import DrawerContent from "./src/Drawer/DrawerContent";
import Cart from "./src/Tab/Cart";
import Order from "./src/Tab/Order";
import Category from "./src/Tab/Category";
import Search from "./src/Tab/search";
import Calls from "./src/BottomTab/Calls";
import Status from "./src/BottomTab/Status";
import Chat from "./src/BottomTab/Chat";

import Logout from "./src/Drawer/Logout";
import { TabBar } from "react-native-tab-view";






const Stack= createStackNavigator()
const Drawer=createDrawerNavigator()
const Tab=createMaterialTopTabNavigator()
const BottomTab=createBottomTabNavigator()

function MyStack(){
    return(
        <Stack.Navigator>

            <Stack.Screen name="Login" 
            component={Login}
            
        options={{headerShown:false}}/>
            <Stack.Screen name="Signup"
            component={Signup}
            options={{headerShown:false}}
            />



            <Stack.Screen 
             name='Flex'
             component={Flex}
             options={{headerShown:false}}
            
            ></Stack.Screen>
            <Stack.Screen
            name="Drawer"
            component={MyDrawer}
            options={{headerShown:false}}></Stack.Screen>
            <Stack.Screen
            name="Bottom"
            component={MyBottomTab}
            options={{headerShown:false}}></Stack.Screen>
            <Stack.Screen
            name='Tab'
            component={MyTab}
            options={{headerShown:false}}></Stack.Screen>

            
        </Stack.Navigator>
    )
}
function MyDrawer() {
    return (
      <Drawer.Navigator drawerContent={(props)=><DrawerContent{...props}/>}>
        
        <Drawer.Screen name="Home"
         component={Home} 
         
         />
        <Drawer.Screen name="Profile"
         component={Profile}
         />
         <Drawer.Screen name="AboutUs"
         component={AboutUs}
         />
         <Drawer.Screen name="Settings"
         component={Settings}
         />
         <Drawer.Screen name="Exit"
         component={Exit}></Drawer.Screen>
         {/* <Drawer.Screen
         name='Logout'
         component={Logout}/> */}
      </Drawer.Navigator>
    );
  }
  function MyTab(){
    return(
        <Tab.Navigator screenOptions={{
            tabBarLabelStyle: { fontSize: 12 },
            // tabBarItemStyle: { width: 100 },
            tabBarStyle: { backgroundColor: 'powderblue' },
            tabBarGap:10
          }}>
            <Tab.Screen name='Cart'
            component={Cart}
            options={{tabBarActiveTintColor:'blue',tabBarInactiveTintColor:'white',
                tabBarIcon:({focused})=>
                focused ? <Ionicons name="cart" color="blue" size={24} />:<Ionicons name="cart" color="white" size={24} />
            }}></Tab.Screen>
            <Tab.Screen name='Search'
            component={Search}
            options={{ tabBarActiveTintColor:'blue',tabBarInactiveTintColor:'white',
                tabBarIcon:({focused})=>
                focused ? <Ionicons name="search" color="blue" size={24} />:<Ionicons name="search" color="white" size={24} />
                
                
            }}>
                
            </Tab.Screen>
            <Tab.Screen name='Category'
            component={Category}
            options={{tabBarActiveTintColor:'blue',tabBarInactiveTintColor:'white',
                tabBarIcon:({focused})=>
                focused ?<Ionicons name="book" color="blue" size={24} />:<Ionicons name="book" color="white" size={24} />
                
            }}></Tab.Screen>
            <Tab.Screen name='Order'
            component={Order}
            options={{tabBarActiveTintColor:'blue',tabBarInactiveTintColor:'white',
                tabBarIcon:({focused})=>
                focused ? <Ionicons name="approve" color="blue" size={24} />:<Ionicons name="approve" color="white" size={24} />
            }}></Tab.Screen>
        </Tab.Navigator>
    )
  }
  function MyBottomTab(){
    return(
        <BottomTab.Navigator screenOptions={{
            tabBarLabelStyle: { fontSize: 12 },
            // tabBarItemStyle: { width: 100 },
            tabBarStyle: { backgroundColor: 'powderblue' },
            tabBarGap:10
          }}>
                <BottomTab.Screen
                name="Chat"
                component={Chat}
                options={{tabBarActiveTintColor:'green',tabBarInactiveTintColor:'white',
                tabBarIcon:({focused})=>
                focused ? <Ionicons name="messenger" color="green" size={24} />:<Ionicons name="messenger" color="white" size={24} />
            }}>
                


                
            </BottomTab.Screen>
            <BottomTab.Screen name="Calls"
                component={Calls}
                options={{tabBarActiveTintColor:'green',tabBarInactiveTintColor:'white',
                tabBarIcon:({focused})=>
                focused ? <Ionicons name="call" color="green" size={24} />:<Ionicons name="call" color="white" size={24} />
            }}></BottomTab.Screen>
            

            <BottomTab.Screen name="Status"
                component={Status}
                options={{tabBarActiveTintColor:'green',tabBarInactiveTintColor:'white',
                tabBarIcon:({focused})=>
                focused ? <Ionicons name="circle" color="green" size={24} />:<Ionicons name="circle" color="white" size={24} />
            }}></BottomTab.Screen>
        </BottomTab.Navigator>
    )
  }

export default function App(){
    return(
        <NavigationContainer>
            <MyStack></MyStack>
        </NavigationContainer>
    )
}