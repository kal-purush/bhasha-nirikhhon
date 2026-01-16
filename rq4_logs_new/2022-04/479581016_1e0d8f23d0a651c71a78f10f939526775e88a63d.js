
import './App.css';
import Navbar from './component/Navbar';
import Data from './component/Data';
import Card from './component/Card';


function App() {
  const cards = Data.map((item) => {
    return (
      <Card
      key ={item.id}
      title = {item.title}
      img = {item.imageUrl}
      location = {item.location}
      description = {item.description}
      googleMapUrl = {item.googleMapsUrl}
      startDate = {item.startDate}
      endDate = {item.endDate}
      />
    );
  })
 return (
   <div>
     <Navbar />
     {cards}
   </div>
 )
}

export default App;