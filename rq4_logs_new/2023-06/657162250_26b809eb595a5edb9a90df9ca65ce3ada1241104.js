import './App.css';
import Header from './Header';
import Goods from './Goods';
import { DiApple } from "react-icons/di";



const headerData = {
  site_name: 'my test site name',
  nav : [
    {'link': 'nav1', 'text': 'mylink'},
    {'link': 'nav2', 'text': 'mylink2'},
    {'link': 'nav3', 'text': 'mylink3'}
  ]
}
const  goods = [
  { 
    'id' : '1',
    'title' : 'apple',
    'cost' : '3000',
    'img' : 'https://w7.pngwing.com/pngs/410/972/png-transparent-apple-brand-logo-macintosh-apple-icon-format-icon-apple-grey-logo-angle-text-heart-thumbnail.png'
  }, 
  {
    'id' : '2',
    'title' : 'pear',
    'cost' : '5000',
    'img' : 'https://freeiconshop.com/wp-content/uploads/edd/pear-outline-filled.png'
  }
]



function App() {
  return (
    <div className="App">
      <Header data = {headerData}/>
      <DiApple />
      {
        goods.map( item => <Goods key={item.id} title={item.title} cost = {item.cost} img = {item.img}/>)
      }
    </div>
  );
}

export default App;