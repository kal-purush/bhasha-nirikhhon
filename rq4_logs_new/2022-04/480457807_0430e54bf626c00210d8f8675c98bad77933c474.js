import {useState,useEffect} from "react";

const key="JHr4HuG4XfQWtScN4muUUWJgPvB17U0b";


export  default function useGetCityCode(city){
  const [listOfCity,setlistOfCity]=useState("");
  const baseUrl='http://dataservice.accuweather.com/locations/v1/cities/autocomplete';
  const qwery=`?apikey=${key}&q=${city}`;

  useEffect(()=>{
    fetch(baseUrl+qwery)
      .then(res=>res.json())
      .then(json=> {setlistOfCity(json)})

  },[city])
return{listOfCity}

};