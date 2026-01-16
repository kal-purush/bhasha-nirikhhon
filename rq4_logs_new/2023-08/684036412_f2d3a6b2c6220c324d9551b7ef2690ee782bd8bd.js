
import React from "react";
import Team from './Team'
function App(){
const [ob, setOb] = React.useState([
    {
      teamname:'Mumbai Indians',
      players:[
        'Rohit',
        'surya',
        'kishan',
        'bhumra',
        'david'
      ]
    },
    {
      teamname:'Royal Challengers Bengaluru',
      players:[
        'Faf',
        'Virat',
        'DK',
        'Maxi',
        'Siraj'
      ]
    },
    {
      teamname:'Sunrisers Hyderabad',
      players:[
        'Kane Williamson',
        'Manish',
        'Bhuvi',
        'Warner',
        'Karan'
      ]
    },
    {
      teamname:'Chennai Super Kings',
      players:[
        'Dhoni',
        'Raina',
        'Jadeja',
        'Rayudu',
        'Ben Stokes'
      ]
    }
  ])
  return (
    <div className="my">
    <h1>welcome to react</h1>
    {
      ob.map((team,i)=>{
        return(
          <Team team={team.players} tname={team.teamname}></Team>
        )
      })
    }
    </div>
  )
}  
export default App;