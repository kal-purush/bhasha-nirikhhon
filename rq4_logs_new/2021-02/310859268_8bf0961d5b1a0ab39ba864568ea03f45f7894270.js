import React, { useContext, useEffect, useState } from "react";
import { WhiskyContext } from "../../context/WhiskyContext";

let ProductDetails = ({
  productID,
  name,
  alcohol,
  price,
  country,
  region,
  destilery,
  descColour,
  descOdour,
  descTaste,
}) => {
  const [stateProductID, setProductID] = useState("");
  if (!productID) {
    setProductID("Produkt ID");
  }

  if (!alcohol) {
    alcohol = 43;
  }

  if (!price) {
    price = "799";
  }

  if (!country) {
    country = "Skottland";
  }

  if (!region) {
    region = "Islay";
  }

  if (!destilery) {
    destilery = "Bowmore";
  }

  if (!descColour) {
    descColour = "Strågul ...";
  }

  if (!descOdour) {
    descOdour = "Lær og brente mandler ...";
  }

  if (!descTaste) {
    descTaste = "Sitrus og plomme ...";
  }
  const { state } = useContext(WhiskyContext);
  useEffect(() => {
    console.log(productID);
  }, [state.searchResults]);

  return (
    <>
      <div className="whiskyDetails">
        <h3>Om whiskyen</h3>
        <ul>
          <li>
            <strong>Navn</strong>
            <input type="text" placeholder={name} />
          </li>
          <li>
            <strong>Produkt ID</strong>
            <input
              type="text"
              placeholder={stateProductID}
              onChange={(event) => setProductID(event.target.value)}
            />
          </li>
          <li>
            <strong>Alkoholstyrke</strong>
            <input type="text" placeholder={alcohol} /> %
          </li>
          <li>
            <strong>Pris</strong>
            <input type="text" placeholder={price} /> 0 NOK
          </li>
        </ul>
        <ul>
          <li>
            <strong>Land</strong>
            <input type="text" placeholder={country} />
          </li>
          <li>
            <strong>Region</strong>
            <input type="text" placeholder={region} />
          </li>
          <li>
            <strong>Destilleri</strong>
            <input type="text" placeholder={destilery} />
          </li>
        </ul>
      </div>

      <div className="whiskyDescription">
        <h3>Beskrivelse</h3>
        <ul>
          <li>
            <strong>Farge</strong>
            <input type="text" placeholder={descColour} />
          </li>
          <li>
            <strong>Lukt</strong>
            <input type="text" placeholder={descOdour} />
          </li>
          <li>
            <strong>Smak</strong>
            <input type="text" placeholder={descTaste} />
          </li>
        </ul>
      </div>
    </>
  );
};
export default ProductDetails;