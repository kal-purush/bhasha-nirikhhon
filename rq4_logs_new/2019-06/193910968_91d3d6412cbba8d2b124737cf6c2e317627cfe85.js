import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';

import css from './LenderAgreement.css';

const LenderAgreement = props => {
    const { rootClassName, className } = props;
    const classes = classNames(rootClassName || css.root, className);

    // prettier-ignore
    return (
        <div className={classes}>
            <p>
                The Askmyneighbor.org DBA Bolelo.org hereinafter referred to as “Community” is 
                a social marketplace facilitating the exchange of personal information and properties
                between people. This socialization shall include reading the profile pages of other 
                members and possibly even contacting them. The Community provides to its member's benefits 
                such as but not exclusive to providing a market place for borrowers and lenders to come 
                together and borrow/lend items for a fee.
            </p> 
   
            <p>The lender agrees</p>
            <ul>
                <li>That s/he is at least 18 years of age with proper identification</li>
                <li>S/he is the owner of the item or has the proper written authority to lend the item being listed on bolelo.</li>
                <li>That the item is not stolen, condemned or subject to legal investigation or inquiry</li>
                <li>Item is stored in a safe location and will transport and operate safely</li>
                <li>Item is not irreplaceable, fragile in nature and the Lender is willing to accept wear/tear from the normal intended use of the item</li>
                <li>Taxes and other government fees levied as a result of monies earned from the Lending transaction is the sole responsibility of each Lender.</li>
                <li>Understands that Bolelo does not allow transaction of perishable items, medical devices that require a prescription or special training, chemicals, weapons, firearms or anything deemed illegal.</li>
                <li>Should a dispute occur due to damage/loss, it is the Lender’s responsibility to seek/resolve amicable solution with the Borrower and/or by filing a claim with bailment insurance, if purchased.</li>
                <li>Understands Bolelo’s role is limited to facilitation of linking Borrower and Lender for the lending/borrowing transaction.  No guarantee/warranty of any kind is offered, expressed or implied.  Bolelo will not mediate nor engage in arbitration.</li>
            </ul>

            <p>
                If the Lender has a dispute with one or more other Members, the Lender shall release the Community (and its officers, directors, agents, subsidiaries, joint ventures and employees) from claims, demands and damages (actual and consequential) of every kind and nature, known and unknown, arising out of or in any way connected with such disputes. 
            </p>  
            <p>         
                Member shall indemnify and hold the Community (and its officers, directors, agents, subsidiaries, joint ventures and employees) harmless from any claim or demand, including reasonable attorneys' fees, made by any third party due to or arising out of Members breach of this Agreement, or Members violation of any law or the rights of a third party.  
            </p>
            <p>
                Member shall choose to retire or delete the published content from the Community’s site and it shall be no longer available or visible to other visitors. Terms regarding the status of the uploaded content shall remain applicable when the Member chooses to terminate the membership. 
            </p>
            <p>
                Member shall not hold Community responsible for other user Members content, actions or inactions.
            </p>
   
        </div>
    );
};

LenderAgreement.defaultProps = {
    rootClassName: null,
    className: null,
};

const { string } = PropTypes;

LenderAgreement.propTypes = {
    rootClassName: string,
    className: string,
};

export default LenderAgreement;