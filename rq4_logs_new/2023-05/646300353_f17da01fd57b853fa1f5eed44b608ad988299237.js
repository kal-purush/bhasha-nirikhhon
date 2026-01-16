import React, {useState, useRef} from 'react'


function AdviceForm(){

    const [randomAdvice, setRandomAdvice] = useState("")
    const [query, setSearchQuery] = useState("")
    // const [message, setMessage] = useState("")


    const queryInput = useRef()
    
    const randomSubmit = () => {

        fetch('https://api.adviceslip.com/advice')

        .then((response) => {
            return response.json() 
        })

        .then((completeData) => {
            console.log(completeData)
            setRandomAdvice(completeData.slip.advice)
        })

        .catch(error => {
            console.log(error)
        })

    }

    const clearRandomInfo = () => {
        // slip.advice.slice(0)
        setRandomAdvice('')
    }


    const searchSubmit = () => {

        const query = queryInput.current.value
       
        fetch(`https://api.adviceslip.com/advice/search/${query}`)

        .then((response) => {
            return response.json()
        })

        .then((completeData) => {
            console.log(completeData)
            setSearchQuery(completeData.slips[0].advice)    
        })

        .catch((error) => {
            console.log(error)
            setSearchQuery("Sorry! Can't help with that!")  
        })
    }

    const clearSearchInfo = () => {
        query.slice(0)
        setSearchQuery('')
    }


    return(
        <div>
            <h2>Daily Piece of Advice</h2>
            <h3>Click Below to Read a Random Piece of Advice</h3>
            <button onClick={randomSubmit}>Click Me</button>
            <button onClick={clearRandomInfo} type="submit">Clear</button>
            <p>{randomAdvice}</p>

            <div>
                <h3>Type a Keyword Below to Search For Advice</h3>
                <input
                    ref={queryInput}
                    required
                    type="text"
                />&nbsp;
                    <button onClick={searchSubmit} type="submit">Results</button>
                    <button onClick={clearSearchInfo} type="submit">Clear</button>
                <p style={{textAlign: 'center'}}>{query}</p>
            </div>
        </div>
    )



}

export default AdviceForm;