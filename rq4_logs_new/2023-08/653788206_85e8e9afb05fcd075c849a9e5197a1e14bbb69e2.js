import React, { Component } from "react";
import Header from "../../components/header/Header";
import Footer from "../../components/footer/Footer";
import TopButton from "../../components/topButton/TopButton";
import "./General.css";
import orig from "../../assests/images/colors_orig.jpeg";
import reduced from "../../assests/images/colors_reduced.jpeg";

class Kmeans extends Component {
  render() {
    return (
      <div className="opensource-main">
        <Header theme={this.props.theme} />
        <div className="content-container">
          <div className="card-header">Kmeans Palette Optimizer</div>
          <p className="card-subheader subTitle">
            Optimizing Artistic Color Palettes through K-means Clustering for
            Resource-Efficient Rendering
          </p>
          <div className="card-text">
            {" "}
            <span style={{ fontWeight: "bold" }}>Problem: </span> Although
            digital art offers a limitless array of color possibilities, the
            realm of physical art is constrained by the finite selection of
            paint colors available to a robotic artist. Moreover, the act of
            switching between colors is not only time-consuming but also
            computationally expensive, thereby posing challenges to efficient
            art creation.
          </div>
          <div className="card-text">
            Developed a K-means clustering algorithm that accepts SVG or CSV
            files as inputs, effectively reducing an infinite range of colors
            down to a manageable set of 'k' distinct colors. This significantly
            accelerates the time required for the robotic artist to complete a
            painting. Additionally, the algorithm dynamically maps all color
            centroids to those within the robot's current available palette,
            transforming previously infeasible artworks into feasible creations.
          </div>
          <div className="card-text">
            Below is an example of this. Due to privacy, this is generated art,
            and not used or sold for any commercial purposes. The left image is
            the original input SVG, while the right image is the color reduced
            SVG. The left image is infeasible with the robot's current palette,
            while the right image is feasible.
          </div>
          <div className="image-container">
            <img
              style={{
                width: "40%",
                height: "auto",
                border: "1px solid black",
                margin: "20px",
              }}
              src={orig}
              alt="My Image"
            />
            <img
              style={{
                width: "40%",
                height: "auto",
                border: "1px solid black",
                marginBottom: 0,
              }}
              src={reduced}
              alt="My Image"
            />
          </div>
        </div>
        <Footer theme={this.props.theme} onToggle={this.props.onToggle} />
        <TopButton theme={this.props.theme} />
      </div>
    );
  }
}

export default Kmeans;