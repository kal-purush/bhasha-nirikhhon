enum ProductType {
  Normal = 'normal',
  Auction = 'auction',
  PreOrder = 'pre-order',
}

const sampleNormalProduct = {
  id: '1',
  type: ProductType.Normal,
  images: [
    '/images/product/product-1-image-0.png',
    '/images/product/product-1-image-1.png',
    '/images/product/product-1-image-2.png',
    '/images/product/product-1-image-3.png',
    '/images/product/product-1-image-3.png',
  ],
  title: 'Pikachu HOLO RARE SM98 Black Star Promo 2017',
  sellerInfo: {
    id: '1',
    name: 'Seller name',
    avatar: '/images/logo/color.svg',
    isVerified: true,
    numFollowers: 74,
  },
  purchase: {
    currentBid: 500,
    numBids: 12,
    price: 700,
  },
  details: {
    condition: 4,
    description:
      'Lorem ipsum dolor sit amet, consectetur adipiscing elit, suradora sed do eiusmod tempor incididunt ut labore et dolore. Elsem ne exercitation ullamco laboris nisi ut aliquip ex ea commodo uta consequat dolor ipsum elit, dolrore gohaper pasum.',
  },
  postedDate: '3 days ago',
};

const generateRandomIntegerNumberFromRange = (min: number, max: number) => {
  return Math.floor(Math.random() * (max - min + 1) + min);
};

const sampleProductsList = [1, 2, 3, 4, 5, 1, 2, 3, 4, 5].map((i) => ({
  ...sampleNormalProduct,
  id: `${i}`,
  type: [ProductType.Normal, ProductType.Auction, ProductType.PreOrder][
    generateRandomIntegerNumberFromRange(0, 2)
  ],
  title: 'Charizard EX',
  images: [`/images/product/product-${i}-image-0.png`].concat(
    sampleNormalProduct.images.slice(1),
  ),
}));

export { sampleProductsList };