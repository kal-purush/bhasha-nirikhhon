import './App.css';
import {Routes,Route} from 'react-router-dom';
import 'bootstrap/dist/css/bootstrap.css';
import Header from './component/Header';
import Main from './component/Main';
import MakeAccount from './component/MakeAccount';
import Deposit from './component/Deposit';
import Withdraw from './component/Withdraw';
import AccountInfo from './component/AccountInfo';
import AllAccountInfo from './component/AllAccountInfo';

function App() {
  return (
    <div className="App">
      <Header/>
      <Routes>
        <Route export path="/" element={<Main/>}/>
        <Route export path="/makeaccount" element={<MakeAccount/>}/>
        <Route export path="/deposit" element={<Deposit/>}/>
        <Route export path="/withdraw" element={<Withdraw/>}/>
        <Route export path="/accountinfo" element={<AccountInfo/>}/>
        <Route export path="/all" element={<AllAccountInfo/>}/>
      </Routes>
    </div>
  );
}

export default App;