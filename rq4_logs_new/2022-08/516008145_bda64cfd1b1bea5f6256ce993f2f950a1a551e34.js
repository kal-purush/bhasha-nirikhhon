import './AddCard.css';
import {IoCloseOutline} from 'react-icons/io5';
import {IoIosAddCircleOutline} from 'react-icons/io';
import {ReactComponent as CTA} from './mismo-cta.svg';

export default function AddCard() {
  return (
    <>
      <h2 className="add-title">New Mismo</h2>
      <section className="add">
        <input type="text" className="product-name" name="product-name" placeholder="Product Name¹" />
        <IoCloseOutline className="exit-icon-1" />
        <IoIosAddCircleOutline className="add-icon-1" />
        <input type="text" className="equivalent-name" name="equivalent-name" placeholder="Equivalent Name²" />
        <IoCloseOutline className="exit-icon-2" />
        <IoIosAddCircleOutline className="add-icon-2" />
        <input type="text" className="product-country" name="product-country" placeholder="Country¹" />
        <IoCloseOutline className="exit-icon-3" />
        <IoIosAddCircleOutline className="add-icon-3" />
        <input type="text" className="equivalent-country" name="equivalent-country" placeholder="Country²" />
        <IoCloseOutline className="exit-icon-4" />
        <IoIosAddCircleOutline className="add-icon-4" />
        <img
          className="image-upload"
          src="https://socialistmodernism.com/wp-content/uploads/2017/07/placeholder-image.png?w=640"
          alt="Upload your image here"
        />
        <CTA className="submit-new-mismo" />
      </section>
    </>
  );
}