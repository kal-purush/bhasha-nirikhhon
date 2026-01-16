import { Image } from "sanity";

export type ProductsType= {
    title: string;
    tag : string;
    _id:string,
    price: string;
    img:  Image;
    category:string,
    // bg: string;
  };