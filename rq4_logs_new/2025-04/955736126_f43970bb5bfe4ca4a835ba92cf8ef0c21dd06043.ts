type CartAttributeItem = AttributeItem & {
  selected: boolean;
};

type CartAttribute = Omit<Attribute, 'items'> & {
  items: CartAttributeItem[];
};

type CartProduct = {
  id: string;
  productId: string;
  name: string;
  price: number;
  currency: string;
  attributes: CartAttribute[];
  quantity: number;
  image: string;
}

interface CartStore {
  isCartOpen: boolean;
  products: CartProduct[];
  openCart: () => void;
  closeCart: () => void;
  toggleCart: () => void;
  addProduct: (product: Omit<CartProduct, 'quantity'>) => void;
  removeProduct: (productId: string) => void;
  increaseQuantity: (productId: string) => void;
  decreaseQuantity: (productId: string) => void;
  updateQuantity: (productId: string, newQuantity: number) => void;
  clearCart: () => void;
}

interface CategoryStore {
  categories: Category[];
  fetched: boolean;
  loading: boolean;
  error: string | null;
  fetchCategories: () => Promise<void>;
}


interface ProductDetailStore {
  product: ProductDetail | null;
  loading: boolean;
  error: string | null;
  fetchProductDetail: (id: string) => Promise<void>;
}


interface ProductStore {
  products: Product[];
  loading: boolean;
  error: string | null;
  fetchProducts: (category: string) => Promise<void>;
}