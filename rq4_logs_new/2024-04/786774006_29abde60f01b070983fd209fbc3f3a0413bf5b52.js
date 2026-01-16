import { food_list } from "../assets/assets";
import { atom }from 'recoil';
import { selector } from "recoil";
import { recoilPersist } from 'recoil-persist'
const { persistAtom } = recoilPersist({
    storage: sessionStorage,
   
  });




export const authenticate=atom({
    key:"authenticate",
    default: false 
})
export const foodListState=atom({
    key: 'foodListState',
    default: food_list
});

export const Itemcount=atom({
    key:"itemcount",
    default: {},
    effects_UNSTABLE: [persistAtom],
    
});

/* Object.entries():
Returns an array of key-value pairs, where each pair is represented as an array containing the key as the first element and the corresponding value as the second element.
The order of the key-value pairs is based on the order in which they were added to the object, which may not necessarily be the same as the order in which they were defined.
Useful when you need both the keys and the values of an object, such as when iterating over them with methods like map() or reduce().
Object.values():
Returns an array containing only the values of the object's properties, omitting the keys.
The order of the values in the array corresponds to the order in which they were added to the object.
Useful when you need to work with just the values of an object, such as when calculating sums or filtering based on values.*/

export const totalcount= selector({
    key:"total",
    get:({get})=>{
        const itemcount=get(Itemcount); //retrieve the value of itemcount is necessary
        return Object.entries(itemcount).reduce((accumlator,[key,currentValue])=>{  //Destructuring: In the reduce() callback function, we use array destructuring to extract the key and value from each pair. So, [key, value] represents the current key-value pair being processed in each iteration of the reduce() function.

            //Using Key and Value: Inside the callback function, we can access both the key and value variables. In this case, we're using the key to check whether it's equal to 'itemcount' and using the value to accumulate the sum, excluding the value associated with the key 'itemcount'.
            if(key !=='itemcount'){
            return accumlator+currentValue;}
            else{
                return accumlator;
            }
        },0)
    }
})




export const addItem= selector({
    key:"additem",
    get: ({get})=>{
        return get(Itemcount);
    },
    // Setitemcount can be a function that takes the previous value and returns a new value
    set: ({ set }, itemid) => {
        set(Itemcount, (prevItemcount) => {
          if (!prevItemcount[itemid]) {
            return { ...prevItemcount, [itemid]: 1 };
          } else {
            return { ...prevItemcount, [itemid]: prevItemcount[itemid] + 1 };
          }
        });
      }
      
})

export const removeItem=selector({
    key:"removeItem",
    get: ({get})=>{
        return get(Itemcount);
    },
    set: ({set}, itemid)=>{
        set(Itemcount,(prevItemcount)=>{
            if(prevItemcount[itemid]>0){
                 return{...prevItemcount, [itemid]:prevItemcount[itemid]-1}
            }
            return prevItemcount
           
        })
    }
});







//console.log(JSON.parse(Cookies.get('cart')))
/*const getCartDataFromLocalStorage = () => {
    const cartData = localStorage.getItem('cart');
    console.log('hi')
    return cartData ? JSON.parse(cartData) : {};
};
export const cartStateAtom = atom({
    key: 'cartStateAtom',
    default: getCartDataFromLocalStorage,
});
  
export const addToCart = (cart, itemId) => {
    console.log('add')
    const updatedCart = { ...cart };
    updatedCart[itemId] = (updatedCart[itemId] || 0) + 1;
    localStorage.setItem('cart', JSON.stringify(updatedCart));
    return updatedCart;
};
  
export const removeFromCart = (cart, itemId) => {
    const updatedCart = { ...cart };
    if (updatedCart[itemId] > 0) {
        updatedCart[itemId]--; // Decrease the quantity by 1
    }
    localStorage.setItem('cart', JSON.stringify(updatedCart));
    return updatedCart;
};*/
  