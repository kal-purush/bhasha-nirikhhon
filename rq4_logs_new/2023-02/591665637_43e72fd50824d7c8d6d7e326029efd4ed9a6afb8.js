
import { useDispatch, useSelector } from 'react-redux';
import { clearCart } from '../utils/cardSlice';
import ItemsCart from './Itemscart';
 const Cart = () => {
    const cartItems = useSelector(store => store.cart.items)
    const dispatch = useDispatch();
    const handleClearCart = () => {
        dispatch(clearCart())
    }
  return (
    <div>
        <h1 className='font-bold text-3xl'>Cart Items - {cartItems.length}</h1>
        <button className='bg-green-50 p-2 m-5' onClick={()=>handleClearCart()}>Clear Cart</button>
        <div className='flex flex-wrap'>
        {
            cartItems.map(item => {
              return  <ItemsCart key={item.id} {...item}/>
            })
        }
        </div>
       
    </div>
  )
}
export default Cart;