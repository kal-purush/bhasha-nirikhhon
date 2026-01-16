import React, { useState } from "react";
import resume from "../../assets/AlexisLangilleResume.pdf";

function Resume() {
  const [resumeLinks] = useState([
    {
      name: "LinkedIn",
      link: "https://www.linkedin.com/in/alexis-l-86b03522a/?originalSubdomain=ca",
    },
    {
      name: "GitHub",
      link: "https://github.com/alangille01",
    },
    {
      name: "Email",
      link: "mailto:langille.alexis@gmail.com",
    },
  ]);

  return (
    <section className="my-5">
      <h2>Resume</h2>

      <p className="my-5">
        Full-stack web developer, with recent Coding Bootcamp completion
        certificate. Experience building MERN stack applications. Highly
        analytical, motivated and skilled at solving programming problems. Work
        well both independently and in a team environment.
      </p>
      <h5>Frontend:</h5>
      <p>
        ReactJS, HTML5, CSS, Responsive Design, Javascript, jQuery, Bootstrap
      </p>
      <h5>Backend:</h5>
      <p>MySQL, ORM, MongoDB, Express, Node, Handlebars, GraphQL, Webpack</p>
      <h5>Education:</h5>
      <p>
        Full-Stack Coding Bootcamp,{" "}
        <span className="italic">University of Toronto EdX</span>
        <br />
        Bachelors of Arts - BA, Political Sciences,{" "}
        <span className="italic">University of Toronto</span>
      </p>
      
    </section>
  );
}

export default Resume;