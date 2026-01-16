import React, { useContext, useEffect } from "react";
import { Context } from "../store/appContext";
import { useParams, Link } from "react-router-dom";
export const Characters = () => {
	const { store, actions } = useContext(Context);
	const {uid}=useParams()
  
	useEffect(() => {
	  console.log(store.people)
	}, [])

return(
		<div className="text-center mt-5">
			<h1 className="text-light">Who got the power?!</h1>
			<div className="card mb-3">
  <div className="row g-0">
    <div className="col-md-4">
      <img src={store.people.imageCharacter}  className="img-fluid rounded-start" alt="..."/>
    </div>
    <div className="col-md-8">
      <div className="card-body">
        <h5 className="card-title">{store.people.name}</h5>
        <p className="card-text">Homeworld:{store.people.homeworld}</p>
        <p className="card-text">Birth year:{store.people.birth_year}</p>
		<p className="card-text">Skin color:{store.people.skin_color}</p>
		<p className="card-text">Mass:{store.people.mass}</p>
		<p className="card-text">Height:{store.people.height}</p>
      </div>
    </div>
  </div>
</div>
			
		</div>
	);
}