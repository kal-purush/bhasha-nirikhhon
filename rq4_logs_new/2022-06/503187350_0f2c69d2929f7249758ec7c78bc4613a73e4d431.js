import axios from "axios"
import { useEffect, useState } from "react"

const UseData = (URL) => {
  const [Data, setData] = useState()
    useEffect(() => {
        axios.get(URL)
        .then(res=>setData(res.data))
        .catch(err=>console.log(err))
      }, [])
  return {Data}
}

export default UseData