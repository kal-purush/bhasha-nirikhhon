const products = [
                {
                    id:1,
                    title:"collar s",
                    price:2000,
                    description:"collar con dije de araña",
                    category:"collares",
                    stock: 30,
                    image:""
                },{
                    id:2,
                    title:"anillo corazon",
                    price:"1800",
                    description:"anillo de esqueleto corazon",
                    category:"anillos",
                    stock: 20,
                    image:""
                },{
                    id:3,
                    title:"pulcera serpiente",
                    price:"1800",
                    description:"pulcera en forma de serpiente",
                    category:"pulceras",
                    stock: 30,
                    image:""
                },{
                    id:4,
                    title:"collar corazon",
                    price:"2500",
                    description:"collar corazon con alas de murcielago ",
                    category:"collares",
                    stock: 23,
                    image:""
                },{
                    id:5,
                    title:"aretes corazon",
                    price:"1500",
                    description:" aretes de corazon con alas",
                    category:"aretes",
                    stock: 10,
                    image:""
                },{
                    id:6,
                    title:"anillo araña",
                    price:"1800",
                    description:"anillo de araña con detalles en negro",
                    category:"anillos",
                    stock: 24,
                    image:""
                },{
                    id:7,
                    title:"aretes manos",
                    price:"1700",
                    description:"aretes aretes de esqueleto",
                    category:"aretes",
                    stock: 40,
                    image:""
                },{
                    id:8,
                    title:"pulcera esqueleto",
                    price:"1500",
                    description:"pulcera de esqueleto",
                    category:"pulceras",
                    stock: 25,
                    image:""
                },{
                    id:9,
                    title:"aretes corazon",
                    price:"1500",
                    description:" aretes de corazon con alas",
                    category:"aretes",
                    stock: 30,
                    image:""
                },{
                    id:10,
                title:"collar corazon",
               price:"2500",
                description:"collar corazon con alas de murcielago ",
                category:"collares",
                stock: 30,
                 image:""
           },{
             id:11,
            title:"pulcera serpiente",
             price:"1800",
            description:"pulcera en forma de serpiente",
             category:"pulceras",
             stock: 30,
            image:""
         },{
            id:12,
           title:"anillo corazon",
            price:"1800",
            description:"anillo de esqueleto corazon",
             category:"anillos",
             stock: 30,
                image:""
            }
        ]
    
    export const getProducts = () => {
        return ((resolve) =>{
            setTimeout(() => {
                resolve ()
            }, 500)
        })
    }

    export const getProductsById = (productId) => {
        return ((resolve) => {
            setTimeout(() => {
                resolve(products.find(prod => prod.id === productId))
            }, 500)
        })
    }