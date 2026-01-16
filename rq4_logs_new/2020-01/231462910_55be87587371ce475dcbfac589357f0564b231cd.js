import React from "react"
import Layout from "../components/layout"
import SEO from "../components/seo"
import "../components/design.css"

const Design = () => (
  <Layout>
    <SEO title="Design" />
    <div class="design">
      <div class="wide-wrapper" style={{ textAlign: "center" }}>
        <h6>HI I’M LAURALEE</h6>
        <h1>
          A web and product designer at KickoffLabs. I’m a wife and mom of two
          beautiful children.
        </h1>
      </div>
      <div
        style={{
          maxWidth: `715px`,
          marginBottom: `1.45rem`,
          marginRight: `auto`,
          marginLeft: `auto`,
        }}
      ></div>
      <div class="skinny-wrapper">
        <p>
          I’m a Product Designer. I have been very happily working remotely
          since 2008. In 2007 I graduated with a masters degree in
          Human-Computer Interaction. I ran a usability team as well as
          developed and taught courses on user center design and user research
          methods at two different Universities - both in person and online.
        </p>
        <p>
          A lot of my work since 2007 has been related to product design. I
          found myself more and more drawn to it. In 2015 I transitioned fully
          to a product design role. I love the symbiotic relationship between
          user research and product design.
        </p>
        <p>
          I’ve found design to be much more than just creating beautiful visuals
          - although that is part of it. Design is understanding the person
          using a product and their goals and helping them be successful. It’s
          also fundamentally about solving business problems.
        </p>
        <p>
          To me, product design is a combination of all three: understanding and
          solving customer and business problems with beautiful visual
          solutions. I absolutely love what I do.
        </p>
      </div>
      <div class="wide-wrapper">
        <p class="quote">
          “Design is a funny word. Some people think design means how it looks.
          But of course, if you dig deeper, it’s really how it works.”
        </p>
        <p class="reference">Steve Jobs</p>
      </div>
      <div class="skinny-wrapper">
        <p>
          In addition to design, I love to create products. I can create pretty
          much any design in HTML and CSS. I have experience with Javascript and
          Ruby on Rails. I created this website in Gatsby and React. I am
          currently starting a new project called CSS Academy.
        </p>
      </div>
      <div class="about-cta">
        <a href="https://lauraleeflores.s3.amazonaws.com/flores-resume.pdf">
          View my resume
        </a>
        <a
          href="mailto:lauralee@lauraleeflores.com?Subject=Website%20inquiry"
          target="_top"
        >
          Send me an email
        </a>
      </div>
    </div>
  </Layout>
)

export default Design