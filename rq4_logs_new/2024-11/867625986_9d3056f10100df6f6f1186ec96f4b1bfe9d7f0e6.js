// src/components/FAQs.js
import React from 'react';
import './FAQs.css';

const FAQs = () => {
  return (
    <section className="faqs-section section" data-aos="fade-up">
      <h3>FAQs</h3>
      <div className="faqs-grid">
        <div className="faq-item" data-aos="flip-up">
          <h4>Can I set up automated email reports?</h4>
          <p>
            Soon! Our automated email reports features is slated to release in November of 2024.
          </p>
        </div>
        <div className="faq-item" data-aos="flip-up" data-aos-delay="100">
          <h4>What counts as an 'activity'?</h4>
          <p>
            An 'activity' in our app is any entry in the 'board-level' activity logs. There are a few redundant activity types (something that would cause double counting) that are fitered out of the calculations.
          </p>
        </div>
        <div className="faq-item" data-aos="flip-up" data-aos-delay="200">
          <h4>Is there a support team available?</h4>
          <p>
            Absolutely! Our dedicated support team is available around the clock to help you out if you are experiencing any issues or would like a hand setting up the app.
          </p>
        </div>
        <div className="faq-item" data-aos="flip-up" data-aos-delay="300">
          <h4>Can I change the board and user configuartion after initial setup?</h4>
          <p>
            Yes, by navigating to the 'settings' gear in the top right of the app, you can use the "Reconfigure" button to update the board/user configuration of the app.
          </p>
        </div>
        <div className="faq-item" data-aos="flip-up" data-aos-delay="400">
          <h4>Is there a limit to how many boards I can track?</h4>
          <p>
            Yes, there's a limit to the number of boards you can track. This value will be dependent on the plan you have. The trial period allows for up to 6 boards, and the free plan allows for up to 2.
          </p>
        </div>
        <div className="faq-item" data-aos="flip-up" data-aos-delay="500">
          <h4>Do dashboard settings or widget filters apply to the app?</h4>
          <p>
            No, our app operates independently of the dashboard settings and widget filters. Boards that are connected to a dashboard with our app on it do not affect the app's configuration.
          </p>
        </div>
      </div>
    </section>
  );
};

export default FAQs;