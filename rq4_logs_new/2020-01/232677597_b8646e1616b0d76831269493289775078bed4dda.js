import React from 'react';

import { Component } from 'react';
import Toolbar from './component/Toolbar/Toolbar';
import ChartJS from './component/ChartJS/ChartJS';
import ChartJS2 from './component/ChartJS2/ChartJS2';
import Button from './component/Button/Button';
import GoogleLogin from './component/GoogleLogin/GoogelLogin';
import SortTB from './component/SortTB/SortTB';
import PieMenu from './component/PieMenu/PieMenu';
import HeatMapp from './component/HeatMap/HeatMap';
import ReactDOM from "react-dom";

const testdata = [
  117594,
  181045,
  153060,
  106519,
  105162,
  95072
];

class App extends Component{
  render() {
    return (
      <div className="App">
        <Toolbar/>
        <Button/>
        <GoogleLogin/>

        <HeatMapp/>
      <table>
        
        <thead>
          <tr>
          <th>Name</th>
          <th>Name2</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><ChartJS data={testdata}/></td>
            <td><ChartJS2/></td>
          </tr>
          <tr>
            <td>team1:{testdata[0]},  team2:{testdata[1]},  team3{testdata[2]},  team4{testdata[3]},  team5{testdata[4]},  team6{testdata[5]}</td>
            <td>team1: Shows why data should be in App.js instead of its components</td>
          </tr>
        </tbody>  
      </table>
      <SortTB/>
      </div>
      
    );
  }
}


export default App;