type Tags = [];

export default class Product {
    constructor(
        public productTitle: string,
        public producerTitle: string,
        public imageSrc: string,
        public productDesc: string,
        public bulkPrice: string,
        public singlePrice: string,
        public expiryDuration: string,
        public productStory: string,
        public productUnique: string,
        public categoryId: string,
        public dateTime: Date,
        public isFeatured: boolean,
        public tags: Tags[],
        public productUnit: string,
    ) {
        this.productTitle = productTitle;
        this.producerTitle = producerTitle;
        this.imageSrc = imageSrc;
        this.productDesc = productDesc;
        this.bulkPrice = bulkPrice;
        this.singlePrice = singlePrice;
        this.expiryDuration = expiryDuration;
        this.productStory = productStory;
        this.productUnique = productUnique;
        this.categoryId = categoryId;
        this.dateTime = dateTime;
        this.isFeatured = isFeatured;
        this.tags = tags;
        this.productUnit = productUnit;
    }
}