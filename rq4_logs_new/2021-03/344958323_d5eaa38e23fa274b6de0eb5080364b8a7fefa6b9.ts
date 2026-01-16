export type ProductsResponse = {
    content: Product[];
    totalPages: number;
  }
  
  export type Product = {
    id: number;
    title: string;
    author: string;
    description: string;
    price: number;
    images: Image[];
    size: string;
    categories: Category[];
    edition: string;
    rating: number;
    quantity: number;
    pages: number;
    status: boolean;
    year: number;
  }
  
  export type Image = {
    id: number;
    imgUrl: string;
    principal: boolean
  }
  
  export type Category = {
    id: number;
    name: string;
    status: boolean;
  }