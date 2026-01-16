import axios from 'axios';

export interface ItemApi {
    id: number;
    title: string;
    price: number;
    description: string;
    category: string;
    image: string;
    rating: { rate: number; count: number }
}

// Here we do the fetch api thing
export const fetchItems = async (): Promise<Array<ItemApi>> => {
    try {
        const response = await axios.get('https://fakestoreapi.com/products/');
        return response.data as ItemApi[];
    } catch (err) {
        console.error('Error fetching items: ', err);
        return [];
    }
}