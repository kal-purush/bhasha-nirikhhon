import React from 'react'

class AppStoreButtons extends React.Component {
  render() {
    return (
      <section id="form-section" className="form-section">
        <div className="form-wrapper">
          <h5>Want to get early access to the private beta?</h5>
          <h6>Find it on your favorite app store.</h6>
          <div className="appstore-wrapper">
            <div className="appstore-fields">
              <a className="button button--appstore">Play Store</a>
              <a className="button button--appstore">App Store</a>
            </div>
          </div>
        </div>
      </section>
    )
  }
}

export default AppStoreButtons