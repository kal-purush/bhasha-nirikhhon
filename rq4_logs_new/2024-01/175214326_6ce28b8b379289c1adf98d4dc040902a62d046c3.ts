
export interface Category {
  title: string;
  icon: string;
  categories?: Category[]
}
  export const categories: Category[] = [
    {
      title: 'bills',
      icon: 'receipt-outline',
    },
    {
      title: 'clothes',
      icon: 'shirt-outline',
    },
    {
      title: 'entertainment',
      icon: 'bowling-ball-outline'
    },
    {
      title: 'food',
      icon: 'restaurant-outline',
      categories: [
        {
          title: 'breakfast',
          icon: 'fast-food-outline'
        },
        {
          title: 'lunch',
          icon: 'fast-food-outline'
        },
        {
          title: 'dinner',
          icon: 'fast-food-outline'
        }
      ]
    },
    {
      title: 'grocery',
      icon: 'cart-outline',
      categories: [

      { title: 'bread', icon: 'cart-outline' },
      { title: 'dairy' , icon: 'cart-outline' },
      { title: 'fruits' , icon: 'cart-outline' },
      { title: 'general' , icon: 'cart-outline' },
      { title: 'vegetables' , icon: 'cart-outline' },
      { title: 'dry fruits' , icon: 'cart-outline' },
      { title: 'other' , icon: 'cart-outline' }
      ]
    },
    {
      title: 'health',
      icon: 'medkit-outline'
    },
    {
      title: 'education',
      icon: 'school-outline'
    },
    {
      title: 'snacks',
      icon: 'fast-food-outline'
    },
    {
      title: 'transport',
      icon: 'train-outline'
    },
    {
      title: 'toiletry',
      icon: 'cut-outline'
    },
    {
      title: 'other',
      icon: 'ellipsis-horizontal-outline'
    },
  ]