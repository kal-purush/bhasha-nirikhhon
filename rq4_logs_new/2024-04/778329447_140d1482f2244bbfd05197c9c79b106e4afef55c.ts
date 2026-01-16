export interface User {
  id: number
  email: string
  password: string
  username: string
  first_name: string
  last_name: string
}

export interface Artwork {
  id: number
  artist_id: number
  category_id: number
  description: string
  image: string
  name: string
  orientation_id: number
  price: number
  style_id: number
  theme_id: number
}

export interface Category {
  id: number
  name: string
}

export interface Style {
  id: number
  name: string
}

export interface Theme {
  id: number
  name: string
}

export interface Orientation {
  id: number
  name: string
}

export interface Favorite {
  user_id: number
  artwork_id: number
}

export interface CartItem {
  user_id: number
  artwork_id: number
  quantity: number
}

export interface Database {
  users: User[]
  artworks: Artwork[]
  categories: Category[]
  styles: Style[]
  themes: Theme[]
  orientations: Orientation[]
  favorites: Favorite[]
  cart: CartItem[]
}