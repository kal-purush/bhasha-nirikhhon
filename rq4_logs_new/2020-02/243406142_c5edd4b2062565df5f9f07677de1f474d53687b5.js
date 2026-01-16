import React from "react";
import Container from "../components/Container";

function NoMatch() {
  return (
    <Container>
      <div className="row">
        <div className="col-md-12">
          <h1>404 - Page Not Found</h1>
          <h5>
            I'm sorry Dave. I'm afraid I can't do that.
            </h5>
        </div>
      </div>
    </Container>
  );
}

export default NoMatch;