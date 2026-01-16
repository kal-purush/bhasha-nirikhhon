import React from "react";
import { Link } from "react-router-dom";
import heart from "../../../assets/images/img/heart (1) 1.png"
import comment from "../../../assets/images/img/comment (2) 1.png"
import view from "../../../assets/images/img/view 1.png"
import styles from "./BlogCard1.module.css";
import blogCard from '../../../utils/BlogCard.json'

const BlogCard1 = () => {
   return (
      blogCard.map((card) => {
         return (
               <div className={styles.card}>
                  <div className={styles.image}><img src={card.img} alt="" /></div>
                  <div className={styles.status}>
                     <li className={styles.type}>{card.status.type}</li>
                     <li className={styles.date}>{card.status.date}</li>
                     <li className={styles.readtime}>{card.status.readtime}</li>
                  </div>
                  <div className={styles.totalcontent}>
                     <h1 className={styles.heading}>{card.title}</h1>
                     <p className={styles.content}>{card.info} <Link to={card.link}>Read More</Link></p>
                  </div>
                  <div className={styles.data}>
                     <div className={styles.like}>
                        <img src={heart} alt="" />
                        <p className={styles.text}>{card.review.like}</p>
                     </div>
                     <div className={styles.comment}>
                        <img src={comment} alt="" />
                        <p className={styles.text}>{card.review.comment}</p>
                     </div>
                     <div className={styles.view}>
                        <img src={view} alt="" />
                        <p className={styles.text}>{card.review.views}</p>
                     </div>
                  </div>
               </div>
         )
      })

   );
};

export default BlogCard1;