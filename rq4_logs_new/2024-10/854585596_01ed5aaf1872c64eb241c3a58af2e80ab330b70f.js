const star = "★";
const noStar = "☆";
const halfStar = "";
console.log(star, noStar, halfStar);

export default function starRating(rating) {
    let stars = "";
    for (let i = 0; i < rating; i++) {
        stars += star;
    }
    return stars;

}