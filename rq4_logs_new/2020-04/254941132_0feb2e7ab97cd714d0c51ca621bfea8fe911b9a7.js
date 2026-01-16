import React from "react"
import {
  CarouselProvider,
  Slider,
  Slide,
  ButtonBack,
  ButtonNext,
  DotGroup,
  Dot,
} from "pure-react-carousel"

import { graphql, useStaticQuery } from "gatsby"

// import Carousel from "react-multi-carousel"
// import "react-multi-carousel/lib/styles.css"
import styles from "./carousel.module.scss"

const responsive = {
  desktop: {
    breakpoint: { max: 3000, min: 1024 },
    items: 3,
    slidesToSlide: 3, // optional, default to 1.
  },
  tablet: {
    breakpoint: { max: 1024, min: 464 },
    items: 2,
    slidesToSlide: 2, // optional, default to 1.
  },
  mobile: {
    breakpoint: { max: 464, min: 0 },
    items: 1,
    slidesToSlide: 1, // optional, default to 1.
  },
}

export default () => {
  const { reviews } = useStaticQuery(graphql`
    query {
      reviews: allContentfulReview(
        sort: { fields: createdAt, order: DESC }
        limit: 5
      ) {
        nodes {
          author
          content {
            content
            childMarkdownRemark {
              html
            }
          }
          link
        }
      }
    }
  `)

  // return (
  //   <Carousel
  //     responsive={responsive}
  //     showDots={true}
  //     infinite={true}
  //     ssr={true}
  //     removeArrowOnDeviceType={["tablet", "mobile", "desktop"]}
  //     autoPlay={true}
  //     autoPlaySpeed={5000}
  //     // customTransition="all .5"
  //     itemClass={styles.item}
  //     centerMode={false}
  //     partialVisible={false}
  //     renderDotsOutside={true}
  //     dotListClass='dots'
  //   >
  //     {reviews.nodes.map(({ author, content, link }) => {
  //       return (
  //         <div class="box">
  //           <p
  //             dangerouslySetInnerHTML={{
  //               __html: content.childMarkdownRemark.html,
  //             }}
  //           />
  //           <p>
  //             <em>- {author}</em>
  //           </p>
  //         </div>
  //       )
  //     })}
  //   </Carousel>
  // )

  return (
    <CarouselProvider
      naturalSlideWidth={600}
      naturalSlideHeight={350}
      totalSlides={reviews.nodes.length}
      isPlaying={true}
      interval={10000}
    >
      <Slider>
        {reviews.nodes.map(({ author, content, link }, i) => {
          return (
            <Slide index={i} style={{ overflow: "auto" }}>
              <div className="content">
                <div
                  dangerouslySetInnerHTML={{
                    __html: content.childMarkdownRemark.html,
                  }}
                />
                <div style={{ textAlign: "right", marginTop: "2em" }}>
                  {" "}
                  <em>- {author}</em>
                </div>
              </div>
            </Slide>
          )
        })}
      </Slider>
      <div className="has-text-centered" style={{ marginTop: "15px" }}>
        {reviews.nodes.map((n, i) => {
          return (
            <Dot
              className="button is-rounded is-small is-success"
              slide={i}
              style={{ margin: "2px" }}
            />
          )
        })}
      </div>
    </CarouselProvider>
  )
}