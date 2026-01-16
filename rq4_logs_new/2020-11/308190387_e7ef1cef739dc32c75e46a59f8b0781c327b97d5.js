import Link from "next/link";
import styles from "./detail.module.css";
import Header from "../../components/Header";
import Footer from "../../components/Footer";

export default function Bracelet001() {
  return (
    <div>
      <head>
        <link
          rel="stylesheet"
          href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css"
          integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm"
          crossorigin="anonymous"
        ></link>
      </head>

      <Header />

      <div className="container">
        <div className="row">
          <div className="col-md-8">
            <img
              className="img-fluid"
              id={styles.imgfluid}
              src="https://d2074lugj8w6nr.cloudfront.net/lh-web-db/product/B516/B516_Kaye_Medical_ID_Bracelet_with_Rose_Gold_Gardenia_Medical_ID_Tag_Overhead_updated_AKR.jpg"
            />
          </div>

          <div className="col-md-4" id={styles.detail}>
            <h1>Vòng tay 001</h1>
            <p className={styles.ma}>Mã: B001</p>
            <h3>1.000.000 đ</h3>
            <button type="submit" className="btn btn-primary" id={styles.buy}>
              Mua ngay
            </button>
            <button
              type="submit"
              className="btn btn-secondary"
              id={styles.gocart}
            >
              Thêm vào giỏ hàng
            </button>
          </div>
        </div>
      </div>

      <Footer />
    </div>
  );
}