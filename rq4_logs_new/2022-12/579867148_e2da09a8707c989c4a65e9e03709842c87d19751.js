import axios from 'axios';
//  import UseState from "./component/UseState";

// import Parent from "./component/Parent";
//  import Students from "./component/Students";

// import UseStateIncrement from "./component/UseStateIncrement";
function App() {

  const getData = async () => {
  try {
    const response = await axios.get(
      `https://famous-quotes4.p.rapidapi.com/random`
    );
    console.log(response)
  } catch (error) {
    console.log(error)
  }
  };


  
  return (
    <div>
    {/* <Students/> */}
{/* <Parent/> */}
<button onClick={getData}>Get</button>
{/* <UseState/> */}
    </div>
  );
}

export default App;