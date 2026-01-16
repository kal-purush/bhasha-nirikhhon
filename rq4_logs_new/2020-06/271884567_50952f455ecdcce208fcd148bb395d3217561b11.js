import React from 'react';

import Layout from '../components/Layout';

const IndexPage = () => (
  <Layout fullMenu>
    <article id="main">
      <header>
        <h2>About Me</h2>
      </header>
      <section className="wrapper style5">
        <div className="inner">
          <h3>Who is Jose Rangel?</h3>
          <p>
            I am 19 years old and born and raised in Venezuela, and came to Miami about 6 years ago with my family.
            I have a passion for technology, fitness, music, and learning. Over the years I been fortunate to have worked in the family business, a music instructor,
            as well as a number of other roles. Most recently I graduated from Wyncode Academy in May of 2020, where I had the opportunity to
            work in teams, collaborate with UX/UI designers, and develop dozens of different projects utilizing various languages and frameworks.
            The stack I learned at Wyncode was React, HTML, CSS, JS, Ruby, Rails, Ruby on Rails, React on Rails, and I am beginning to learn Python.
            Since Wyncode graduation I have been spearheading a freelance web dev agency startup with a fellow graduate, and have completed various client projects
            with a few more in the pipeline.
          </p>
          <hr />

          <h4>What I am Looking For</h4>
          <p>
            Something that really stood out to me about
            Wyncode was the culture and the mindset of "Never Stop Learning". I am constantly striving to be the best version of myself and to never stop growing, and am searching for
            a company that is the right fit for my goals. Having a supportive environment where everyone works together collectively and help each other achieve business goals is something I would
            love to be a part of. I am confident that I would have an immediate and positive impact on the team, not only because I have experience building web applications using a variety
            of programming languages and have worked collaboratively with UX/UI teams, but also because my professional work ethic and personality qualities. I know that my commitment to excellence,
            and passion for creating innovative, technical, and functional solutions to complex problems would make me an asset to any company. My familiarity with database management (SQL, PostgreSQL),
            experience with JavaScript/Sass frameworks (React), APIs and 3rd party technology integration, Git and Github, working collaboratively with teams and
            design systems would make me a great fit for the right company. I am open to remote work as well as relocation, with an interest in Denver, Philadelphia, Washington D.C., and Seattle.
               </p>
        </div>
      </section>
    </article>
  </Layout>
);

export default IndexPage;