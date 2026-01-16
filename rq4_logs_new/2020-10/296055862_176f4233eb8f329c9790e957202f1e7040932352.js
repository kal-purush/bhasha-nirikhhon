import React from 'react';
import PropTypes from 'prop-types';

const FreeParkRidePopup = ({ data }) => {
  console.info('INFO', data)
  const {
    nom_complet,
    capacite_voiture,
  } = data.properties;

  return (
    <div className="popup-wrapper">
      <img
        src={`${process.env.PUBLIC_URL}/img/parking-symbol@2x.png`}
        alt="Parking icon"
      />
      <div className="popup-informations">
        <h3 className="popup-park-name">{nom_complet}</h3>
        <p>
          Nombre de places totales : <strong>{capacite_voiture}</strong>
        </p>
      </div>
    </div>
  );
};

FreeParkRidePopup.propTypes = {
  data: PropTypes.object.isRequired,
};

export default FreeParkRidePopup;