import React from 'react'
import PropTypes from 'prop-types'
// import { kebabCase } from 'lodash'
// import { Helmet } from 'react-helmet'
import { graphql } from 'gatsby'
import Layout from '../components/Layout'
import Content, { HTMLContent } from '../components/Content'
import { motion } from 'framer-motion'
import { useInView } from 'react-intersection-observer'

const AnimatedPicture = ({ delay }) =>
{
  const newVarients = {
    // hidden: { opacity: 0 },
    visible: {
      opacity: [0, 1, 0],
      transition: {
        duration: 7,
        delay: delay,
        repeat: Infinity,
        repeatDelay: 9
      }
    }
  }

  return (
    <motion.div
      className='has-background-primary mb-5'
      style={{ width: '100%', height: '150px', borderRadius: '5px' }}
      // initial="hidden"
      animate="visible"
      variants={newVarients}
    >

    </motion.div>
  )
}

export const SessionPageTemplate = ({
  content,
  contentComponent,
  // description,
  // tags,
  title,
  // helmet,
}) =>
{
  const PostContent = contentComponent || Content

  const [galleryRef, galleryInView] = useInView({
    triggerOnce: true,
    threshold: 0.2
  })

  const galleryVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.1
      }
    }
  }
  const galleryItemVariants = {
    hidden: { opacity: 0 },
    visible: { opacity: 1 }

  }

  const galleryImages = []
  for (var i = 0; i < 30; i++)
  {
    galleryImages[i] = i;
  }

  return (
    <section className="section">
      {/* {helmet || ''} */}
      <div className="container content">
        <div className="columns">
          <div className='column is-2' >
            <div className='movingimagewrapper' >
              <AnimatedPicture delay={0} />
              <AnimatedPicture delay={8} />
              <AnimatedPicture delay={4} />
              <AnimatedPicture delay={12} />
            </div>
          </div>
          <div className="column is-8">
            <div className="section">
              <h1 className="title is-size-1 has-text-weight-bold has-text-centered is-family-secondary">
                {title}
              </h1>
              <PostContent content={content} className="has-text-centered is-size-5" />
            </div>

          </div>
          <div className='column is-2' >
            <div className='movingimagewrapper' >
              <AnimatedPicture delay={14} />
              <AnimatedPicture delay={2} />
              <AnimatedPicture delay={10} />
              <AnimatedPicture delay={6} />
            </div>
          </div>
        </div>
        <motion.div
          ref={galleryRef}
          animate={galleryInView ? 'visible' : ''}
          variants={galleryVariants}
          initial='hidden'
        >
          <h3 className='title is-size-2 has-text-weight-bold has-text-centered is-family-secondary'>Gallery</h3>
          <div className='columns is-multiline'>
            {galleryImages.map(item => (
              <div key={item} className='column is-one-quarter-desktop is-half-tablet'>
                <motion.div
                  variants={galleryItemVariants}
                  className='has-background-primary'
                  style={{ width: '100%', height: '200px', borderRadius: '5px' }}>

                </motion.div>
              </div>
            ))}
          </div>
        </motion.div>
      </div>
    </section>
  )
}

SessionPageTemplate.propTypes = {
  content: PropTypes.node.isRequired,
  contentComponent: PropTypes.func,
  // description: PropTypes.string,
  title: PropTypes.string,
  // helmet: PropTypes.object,
}

const SessionPage = ({ data }) =>
{
  const { markdownRemark: post } = data

  return (
    <Layout>
      <SessionPageTemplate
        content={post.html}
        contentComponent={HTMLContent}
        // description={post.frontmatter.description}
        // helmet={
        //   <Helmet titleTemplate="%s | Blog">
        //     <title>{`${post.frontmatter.title}`}</title>
        //     <meta
        //       name="description"
        //       content={`${post.frontmatter.description}`}
        //     />
        //   </Helmet>
        // }
        // tags={post.frontmatter.tags}
        title={post.frontmatter.title}
      />
    </Layout>
  )
}

SessionPage.propTypes = {
  data: PropTypes.shape({
    markdownRemark: PropTypes.object,
  }),
}

export default SessionPage

export const pageQuery = graphql`
  query SessionPageByID($id: String!) {
    markdownRemark(id: { eq: $id }) {
      id
      html
      frontmatter {
        title
      }
    }
  }
`