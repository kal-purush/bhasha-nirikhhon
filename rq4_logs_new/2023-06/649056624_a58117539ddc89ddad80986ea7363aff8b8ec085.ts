type Food = {
  id: number
  name: string
  price: number
  image_path: string
}

type OrderItem = {
  id: number
  amount: number
  food_id: number
  order_item_extras: OrderItemExtras
}

type OrderItemExtras = {
  id: number
  amount: number
  extra_id: number
  order_item_id: number
}

type Order = {
  id: number
  name: string
  order_type: number
  order_status: number
  order_items: OrderItem[]
}