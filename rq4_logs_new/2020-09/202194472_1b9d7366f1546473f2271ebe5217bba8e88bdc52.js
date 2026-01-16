import React from 'react';
import privacyPolicyContent from '../../content/privacy-policy.md';

import Footer from '../Footer';

const PrivayPolicyPage = () => {
    return (
        <>
            <section id="privacy-policy"> 
                <div className="page" id="privacy-policy-content" dangerouslySetInnerHTML={{__html: privacyPolicyContent }}/>
            </section>
            <Footer/>
        </>
    );
};

export default PrivayPolicyPage;