import React, { createContext, useState} from 'react';
import axios from 'axios';


const StarshipsContext = React.createContext();

const StarshipsProvider = ({children}) => {
    const [ starships, setStarships ] = useState(null);
    const [ isLoading, setIsLoading] = useState(false);
    const [ error, setError] = useState(null);
    const [ page, setPage ] = useState(1);
    const [ starshipURL, setStarshipURL] = useState(null);
    const [ starshipDetails, setStarshipDetails ] = useState(null);

    const getStarships = async () => {
        setIsLoading(true);
        try {
          const response = await axios.get('https://swapi.dev/api/starships/', {
                  params: {
                  page: page
                  }
                })
            error && setError(false);
          setStarships(response.data.results);
        }
        catch(error){
            setError(error.message);
        }
        finally {
            setIsLoading(false);
        }
      
        // axios.get('https://swapi.dev/api/starships', {
        //   params: {
        //     page: 1
        //   }
        // })
        // .then((response)=> {
        //   setStarships(response.data.results);
        // })
        //.catch((error) => console.log(error));
        //.finally(()=> setIsLoading(false))
      };

      
    const getStarshipById = async () => {
        console.log(starshipURL)
        setIsLoading(true);
        try {
          const response = await axios.get(starshipURL)
          error && setError(false);
          setStarshipDetails(response.data);
        }
        catch(error){
            setError(error.message);
        }
        finally {
            setIsLoading(false);
        }
      
        // axios.get(starshipURL)
        // .then((response)=> {
        //   setStarships(response.data);
        // })
        //.catch((error) => console.log(error));
        //.finally(()=> setIsLoading(false))
      };
    
  return (
  <StarshipsContext.Provider 
    value={{
        starships,
        isLoading,
        error,
        page,
        starshipDetails,
        setPage,
        getStarships,
        setStarshipURL,
        getStarshipById
    }}
   >
    {children}
   </StarshipsContext.Provider>
  )
}

export { StarshipsProvider };
export default StarshipsContext;