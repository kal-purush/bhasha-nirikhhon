import { SellerType } from '../main/types';

type UserType = {
    id: string;
    name: string;
    email?: string;
    phone?: string;
};

type ProductDetailType = {
    id: string;
    createdDate: string;
    updateDate: string;
    title: string;
    description: string;
    img: string[];
    price: number;
    category: string;
    brand?: string;
    color?: string[];
    size?: string;
    postUser: UserType;
    likes: number;
    count: number; // view count
};

export type { ProductDetailType };