// @flow
import React from 'react';
import PropTypes from 'prop-types';

// refactor props and state

import styles from './index.css';
import DataCard from '../DataCard'

const handleSubmit = (event, fetchColdCallData) =>{
  fetchColdCallData('');
  event.preventDefault();
}


const ViewDb = ({ queriedData, showContactStatus = false })=> (
  <div>
    <div className={styles.container} data-tid="container">
      <h2>View the data</h2>
      <div className={styles.dataCardContainer} >
        {
        queriedData.length > 1 && queriedData.map((data) =>(<DataCard 
          key={data._id} 
          dateOfPurchase={data.dateOfPurchase}
          email={data.user_email}
          dateToContact={data.dateToContact}
          products={data.order}
          name={data.user.name}
          phoneNumber={data.user.phone_number}
          acceptsMarketing={data.user.accepts_marketing}
          contactedStatus={showContactStatus&& null}
          dateOfContact={data.dateOfContact}
          
        />))
      }
      </div>
    </div>   
  </div>
  );

ViewDb.defaultProps = {
  queriedData: []
};
ViewDb.propTypes = {
  fetchColdCallData: PropTypes.func.isRequired,
  queriedData: PropTypes.arrayOf()
}
  
export default ViewDb
