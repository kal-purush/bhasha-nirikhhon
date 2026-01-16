import React from 'react';

export default function AboutForm() {
  return (
    <section id="section-contact">
      <div className="row">
        <div className="col span_1_of_2">
          <div className="form__details">
            <div className="form__content">
              <img
                src="https://res.cloudinary.com/shopping-assets/image/upload/v1584784189/mail_icon_bkwzct.png"
                alt="Mail icon"
              />
              <h3 className="heading-title--main--medium">
                Newsletter &amp; Get Updates
                <span className="form__desc">
                  Sign up for our newsletter to get up-to-date from us
                </span>
              </h3>
            </div>
          </div>
        </div>
        <div className="col span_1_of_2">
          <form className="form u-d-flex-center" method="dialog">
            <input
              className="form__input"
              type="email"
              name="email"
              id="email"
              placeholder="Enter your email here..."
              autoComplete="off"
            />
            <button type="submit" className="btn btn--secondary">
              Submit
            </button>
          </form>
        </div>
      </div>
    </section>
  );
}