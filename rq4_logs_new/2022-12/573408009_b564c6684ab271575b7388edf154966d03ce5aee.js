import React, { useState } from 'react';
import { Header, Sidebar} from './components';
import {Login} from "./pages"

const App = () => {
  const [auth, setAuth] = useState(true)

  if(!auth){
    return (
      <main>
        <Login setAuth={setAuth}/>
      </main>
    )
  }

  return (
    <main>
      <Header />
      <Sidebar />
    </main>
  );
}

export default App;