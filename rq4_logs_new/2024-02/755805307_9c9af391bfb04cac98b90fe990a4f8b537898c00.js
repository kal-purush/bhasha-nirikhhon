import React from 'react'
import { useState } from 'react'

function pro(){
    const [pro, setPro] = useState({

        Image: "src.jpg",
        Price: 0,
        Address: "str",
        Mortgage: 0,
        Advice: "str2",
        Area: 0,
        Townhall: false,
        Condo: false,
        Apparte: false,

});
}

const updateImage = () => {
    setPro(previousState => {
        return {...previousState, Image: null}
    });
}

const updatePrice = () => {
    setPro(previousState => {
        return {...previousState, Price: null}
    });
}

const updateAddress = () => {
    setPro(previousState => {
        return {...previousState, Address: null}
    });
}

const updateMortgage = () => {
    setPro(previousState => {
        return {...previousState, Mortgage: null}
    });
}

const updateAdvice = () => {
    setPro(previousState => {
        return {...previousState, Advice: null}
    });
}

const updateArea = () => {
    setPro(previousState => {
        return {...previousState, Area: null}
    });
}

const IsTownhall = () => {
    setPro(previousState => {
        return {...previousState, Townhall: null}
    });
}

const IsApparte = () => {
    setPro(previousState => {
        return {...previousState, Apparte: null}
    });
}

const IsCondo = () => {
    setPro(previousState => {
        return {...previousState, Condo: null}
    });
}





export default function InfoPage() {
  return (
    <div>
        

        








    </div>
  )
}