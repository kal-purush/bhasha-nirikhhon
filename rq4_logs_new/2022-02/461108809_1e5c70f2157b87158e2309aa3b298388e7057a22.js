import React, { useEffect, useState } from 'react';
import './App.css'

function App() {

  const [prizes, setPrizes] = useState([]);
  const [displayData, setDisplayData] = useState([]);
  const [selectedYear, setSelectedYear] = useState("0");
  const [selectedCategory, setSelectedCategory] = useState("0");
  const [years, setYears] = useState([]);
  const [categories, setCategories] = useState([]);
  const [doubleActive, setDoubleActive] = useState(false);
  const [doubleActiveData, setDoubleActiveData] = useState([]);

  function capitalizeFirstLetter(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
  }

  useEffect(() => {
    const data = fetch('http://api.nobelprize.org/v1/prize.json')
      .then(response => response.json()
        .then(responseJson => {
        
          setPrizes(responseJson.prizes)

          setDisplayData(responseJson.prizes)

          let y = [];
          for(let i=2018; i>1899; i--){
            y.push(i);
          }
          setYears(y);

          const uniques = [...new Set(responseJson.prizes.map(item => item.category))]
          setCategories(uniques);

          var temp = [];
          var multipleArray = [];

          responseJson.prizes.map((item) => {
            item?.laureates?.map(laureate => {
              if(temp.includes(laureate.id)){
                if(laureate.surname){
                  multipleArray.push(laureate)
                }
              } else {
                temp.push(laureate.id)
              }
            })
          })
          setDoubleActiveData(multipleArray)

        }))
      .catch(err => console.error(err))
  },[])

  const handleChangeYear = (e) => {
    const value = e.target.value;
    setSelectedYear(value);
    const data = prizes.filter(el => el.year === value)
    setDisplayData(data);
  }

  const handleChangeCategory = (e) => {
    const value = e.target.value;
    setSelectedCategory(value);
    const data = prizes.filter(el => el.category === value)
    setDisplayData(data);
  }

  const handleReset = () => {
    setSelectedCategory("0")
    setSelectedYear("0")
    setDisplayData(prizes)
  }

  return (
    <div className="App">
      <h1>NOBEL PRIZE WINNERS</h1>
      <div className="action-tab">
        <div className="action-btn" onClick={() => setDoubleActive(!doubleActive)}>Show Multiple Times Winners / Back</div>
        <div className="filters">
          <select defaultValue={selectedYear} onChange={handleChangeYear}  className="action-btn" name="Year">
            <option value="0" disabled hidden>
              Choose Year
            </option>
            {years.map(year => (
              <option value={year}>{year}</option>
            ))}
          </select>
          <select defaultValue={selectedCategory} onChange={handleChangeCategory} className="action-btn" name="Category">
            <option value="0" disabled hidden>
              Choose Category
            </option>
            {categories.map(cate => (
              <option value={cate}>{capitalizeFirstLetter(cate)}</option>
            ))}
          </select>
          <div className="action-btn" onClick={handleReset}>Reset</div>
        </div>
      </div>
      {!doubleActive ? <div className="prize-display">
        {displayData.map(prize => (
          <div className="prize-card">
            <div className="prize-card-header">
              <p className="card-header-text">{prize.year}</p>
              <p className="card-header-text">{capitalizeFirstLetter(prize.category)}</p>
            </div>
            <div>
              {prize?.laureates?.map(laureate => (
                <div>
                  <p>{laureate.firstname} {laureate.surname ? ` ${laureate.surname}` : ''}</p>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div> : 
      <div className="prize-display">
        {doubleActiveData.map(laureate => (
          <div className="prize-card" style={{width: "40%"}}>
            <div className="prize-card-header">
              <p className="card-header-text">{laureate.firstname} {laureate.surname}</p>
              {/* <p className="card-header-text">{capitalizeFirstLetter(prize.category)}</p> */}
            </div>
            <div>
                  <p>{laureate.motivation}</p>
            </div>
          </div>
        ))}
      </div>}
    </div>
    
  );
}

export default App;