const ManagerPage = () => <h1>Manager</h1> 
 


const App = ()=> {       
    // const routes = [
    //     {path:"/user", component:UserPage},
    //     {path:"/admin/:id", component:AdminParamPage},
    //     {path:"/admin", component:AdminPage},
    //     {path:"/gm", component:LoginPage},
    //     {path:"/manager", component:ManagerPage},
    //     {path:"/", component:LoginPage}, 
    // ];
    // console.log(routes)
    return (
        <div>  
            <HashRouter>   
                <Switch>  
                    <Route path="/user">
                        <UserHomePage/>
                    </Route> 
                    <Route path="/admin">
                        <AdminHomePage/>
                    </Route>
                    <Route path="/gm">
                        <LoginPage/>
                    </Route>
                    <Route path="/manager">
                        <ManagerPage/>
                    </Route>
                    <Route exact path="/">
                        <LoginPage/>
                    </Route>  
                    
                     <Route>
                        <Error404Page/>
                    </Route>
                </Switch> 
            </HashRouter>   
        </div>
    )
}
ReactDOM.render(<App/>,document.getElementById("root"));