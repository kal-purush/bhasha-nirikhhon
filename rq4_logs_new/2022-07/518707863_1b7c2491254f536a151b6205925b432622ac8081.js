import React, { useContext, useEffect } from "react";
import Layout from "../components/common/Layout";
import {Box, Typography } from '@material-ui/core';
import {Routes, Route} from 'react-router-dom'
import SignIn from "../Pages/Auth/SignIn";
import SingleArticlePage from "../Pages/SignleArticlePage";
import Home from "../Pages/Home";
import CreateArticle from "../Pages/CRUD/CreateArticle";
import EditeArticle from "../Pages/CRUD/EditeArticle";
import SignUp from "../Pages/Auth/SignUp";
import Profile from "../Pages/Profile";
import Articles from "../Pages/Articles";
import Loader from "../components/common/Loader";
const App =()=>{  
    return (
        <>
       <Loader/>
       <Layout>
        <Routes>
            <Route path='/' element={
               <Home/>
            }/>
            <Route path='/Articles' element={
                <Articles/>
            } />
            <Route path='/Add' element={
                <CreateArticle/>
            } />
             <Route path='/Edite' element={
                <EditeArticle/>
            } />
             <Route path='/Delete' element={
                <Typography sx={{marginTop:'4rem'}}>deleted</Typography>
            } />
            <Route path='/SignIn' element={
                <SignIn/>
            } />
            <Route path='/SignUp' element={
                <SignUp/>
            } />
            <Route path='/profile' element={
              <Profile/>
                
            } />
            <Route path='/signleArticles/:SingleArticlePage' element={
                <SingleArticlePage/>
            } />
           
        </Routes>
       </Layout>
       </>
    )
};

export default App