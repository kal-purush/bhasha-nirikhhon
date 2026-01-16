// 求和
const arr = [4,1,7,10,8];
const addTotally = arr.reduce((a,b) => a + b)//30
console.log(1111,addTotally)

arr.reduce((pre,value,index,array) => {
    // pre默认是传进来的值或者数组的第一个，value是数组的第二个，pre是本函数的return 值
    console.log(2222,pre,value)
    // 4,1
    // undefined,7
    // undefined,10
    // undefined,8
})
arr.reduce((pre,value,index,array) => {
    // pre默认是传进来的值或者数组的第一个，value是数组的第二个，pre是本函数的return 值
    console.log(33333,pre,value)
    // 4,1
    // 99,7
    // 99,10
    // 99,8
    return 99 
})
arr.reduce((pre,value,index,array) => {
    // pre默认是传进来的值或者数组的第一个，value是数组的第一个，pre是本函数的return 值
    console.log(444444,pre,value)
    // 0,4
    // 88,1
    // 88,7
    // 88,10
    // 88,8
    return 88 
},0)


//求4在数组中出现的次数
const arr1 = [4,5,6,4,4,4,4,9]
const number = arr1.reduce((totally,value) => {
    totally += value == 4?1:0
    return totally
},0)
console.log(404040,number)//5



// 求最贵的商品
const arr2 =[
    {name:'ipad',price:2000},
    {name:'mac',price:12000},
    {name:'iphone',price:6000}
]
// 求最贵的商品
function maxPrice(arr2){
    return arr2.reduce((pre,value) => pre.price > value.price?pre:value)
}
console.log(5555,maxPrice(arr2))// { name: 'mac', price: 12000 }


// 求商品价格的总和
function totallPrice(arr2){
    return arr2.reduce((totally,value) => {
        return totally += value.price
         
    },0)
}
console.log(7777,totallPrice(arr2))//20000


//获取价格超过6000的商品名称
function priceSix(arr2,price){
    return arr2.reduce((newArr,value) => {
        if(value.price >= price)newArr.push(value.name)
        
        return newArr
    },[])
}

console.log(8888,priceSix(arr2,6000))//20000



//去重
const arr3 = [4,6,5,8,4,9,9,3]
function deleteRepeat(arr3){
    return arr3.reduce((newArr,value) => {
        //这个数组不含这个值
        if(newArr.includes(value) === false)newArr.push(value)
        return newArr
    },[])
}

console.log(9999,deleteRepeat(arr3))//20000



// 去重商品
const arr4 =[
    {name:'iphone',price:6000},
    {name:'ipad',price:2000},
    {name:'mac',price:12000},
    {name:'iphone',price:6000},
    {name:'ipad',price:2000},
]
function deleteRepeatGoods (arr4){
    return arr4.reduce((arr,cur) => {
        const find = arr.find( item => item.name == cur.name)
        if(!find)arr.push(cur)
        return arr
    },[])
}
console.log(10101010,deleteRepeatGoods(arr4))