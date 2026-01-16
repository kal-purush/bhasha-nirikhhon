import React from "react";
import ford from "../images/ford.jpg";
import mclaren from "../images/mclaren.jpg";
import mercedes from "../images/mercedes.jpg";
import am from "../images/am.jpg";
import ferrari from "../images/ferrari.jpg";
import at from "../images/AT.jpg";

const Ford = ({ value }) => {
  let car_image = null
  let title = null
  let car_body = null
  let link = null;

  switch (value) {
    case "Aston Martin":
      car_image = am;
      title = "Aston martin Cognizant F1 team";
      car_body =
        "Aston Martin is a British car manufacturer that has participated in Formula One in various forms.";
      link = "https://www.astonmartinf1.com/en-GB/";
      break;
    case "Ferrari":
      car_image = ferrari;
      title = "Scuderia Ferrari F1";
      car_body =
        "Scuderia Ferrari S.p.A. is the racing division of luxury Italian auto manufacturer Ferrari and the racing team that competes in Formula One racing.";
      link = "https://www.ferrari.com/en-EN/formula1";
      break;
      case "Mercedes":
        car_image = mercedes;
        title = "Mercedes AMG Petronas F1 Team";
        car_body =
          "Daimler AG, through its Mercedes-Benz brand, has been involved in Formula One as both team owner and engine manufacturer for various periods since 1954.";
        link = "https://www.mercedesamgf1.com/en/";
      break;
      case "AlphaTauri":
        car_image = at;
        title = "Scuderia AlphaTauri";
        car_body =
          "Scuderia AlphaTauri, or simply AlphaTauri, and competing as Scuderia AlphaTauri Honda, is an Italian Formula One racing team and constructor.";
        link = "https://www.scuderiaalphatauri.com/en/";
      break;
      case "Mclaren":
        car_image = mclaren;
        title = "McLaren";
        car_body =
          "McLaren Racing Limited is a British motor racing team based at the McLaren Technology Centre in Woking, Surrey, England.";
        link = "https://www.mclaren.com/racing/";
      break;

    default:
      break;
  }

  return (
    <div>
      <div className="card" style={{ width: 500 }}>
        <img
          src={car_image}
          className="card-img-top"
          alt="car image"
          style={{ width: 500, height: 305 }}
        />
        <div className="card-body">
          <h5 className="card-title">{title}</h5>
          <p className="card-text">
           {car_body}
          </p>
          <a href={link} className="btn btn-primary">
            Goto Website
          </a>
        </div>
      </div>
    </div>
  );
};

export default Ford;