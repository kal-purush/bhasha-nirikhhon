const buttonContainer = document.querySelector(".button-container")



const regionsCountries = {
    asia: [],
    europe: [],
    africa: [],
    americas: [],
    world: [],
  }
  const regions = ['asia', 'europe', 'africa', 'americas'];
  const world = {
    asia: {},
    europe: {},
    africa: {},
    americas: {}
  }
  async function getCountriesByRegion(region){
    // if (regionsCountries[region]){

      try{
        const result = await fetch(`https://intense-mesa-62220.herokuapp.com/https://restcountries.herokuapp.com/api/v1/region/${region}`);
        const data = await result.json();
        data.forEach(country => {
          regionsCountries[region].push(country.cca2);
          world[region][country.cca2] = {}
          fetchCovidData(region,country.cca2,country.name.common);
        })
      }
      catch(err) {console.log(err);}
    // }
    console.log(regionsCountries );
    getCovidData(world[region]);
    }
    async function fetchCovidData(region,country,name) {
      try{
        const result = await fetch(`https://intense-mesa-62220.herokuapp.com/http://corona-api.com/countries/${country}`);
        const data = await result.json();
        const {
          confirmed,
          deaths,
          critical,
          recovered,
        } = data.data.latest_data;
        const {
          confirmed: newCases,
          deaths: newDeaths
        } = data.data.today
        buildWorldObj(region, country, name, confirmed, newCases, deaths, newDeaths, recovered, critical);
      }
      catch(err) {console.log(err);}
    }
    function getCovidData(countries){
      console.log(countries);
    }
    function buildWorldObj(region,country,name,totalCases,newCases,totalDeaths,newDeaths,totalRecovered,inCritical){
      world[region][country]['name'] = name;
      world[region][country]['totalCases'] = totalCases;
      world[region][country]['newCases'] = newCases;
      world[region][country]['totalDeaths'] = totalDeaths;
      world[region][country]['newDeaths'] = newDeaths;
      world[region][country]['totalRecovered'] = totalRecovered;
      world[region][country]['inCritical'] = inCritical;
    }
    // getCountriesByRegion(regions[0]);


    buttonContainer.addEventListener("click" , (e)=>{
      if(e.target.type === "button"){
            console.log(e.target.textContent);
            console.log(Object.keys(world).length);
            console.log("hi")
        getCountriesByRegion(e.target.className);

        }
    
        
    })