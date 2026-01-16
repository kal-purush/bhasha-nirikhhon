import React from "react";
import { useState, useEffect, Children, cloneElement } from "react";
import { FaChevronLeft, FaChevronRight } from "react-icons/fa";
import "../style/Carusel.css";

const Carusel = ({ children }) => {
  const [pages, setPages] = useState([]);
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    setPages(
      Children.map(children, (child) => {
        return cloneElement(child, {
          style: {
            height: "100%",
            minWidth: "100%",
            maxWidth: "100%",
          },
        });
      })
    );
  }, []);

  const prevSlider = () => {
    setOffset((currentOffset) => {
      const newOffset = currentOffset + "100%";
      return newOffset;
    });
  };
  const nextSlider = () => {
    setOffset((currentOffset) => {
      const newOffset = currentOffset - "100%";
      return newOffset;
    });
  };

  return (
    <div className="carusel_container">
      <div className="carusel_window">
        <FaChevronLeft className="arrow arrow_left" onClick={prevSlider} />
        <div
          className="carusel_all_pages_container"
          style={{
            transform: `translateX(${offset})`,
          }}
        >
          {children}
        </div>
        <FaChevronRight className="arrow arrow_right" onClick={nextSlider} />
      </div>
    </div>
  );
};

export default Carusel;