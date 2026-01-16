import Review from '../models/Review';
import Product from '../models/Product';

const recalculateProductRatingHook = async (review: Review) => {
  const product = await Product.findByPk(review.productId);

  
  const reviews = await Review.findAll({
    where: { productId: product.id },
  });

  const ratings = reviews.map((review) => review.rating);
  const averageRating = ratings.reduce((acc, current) => acc + current, 0) / ratings.length;

  product.rating = averageRating;
  await product.save();
};

export recalculateProductRatingHook;