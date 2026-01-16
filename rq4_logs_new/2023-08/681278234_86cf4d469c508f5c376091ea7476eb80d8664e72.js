import Dash from '../../components/dash/Dash'
import Error from './Error.jsx'
import Navigation from '@/app/components/navigation/Navigation'
import Sellcard from '@/app/components/Sellcard/Sellcard'
async function getCategories() {
    const res = await fetch('https://dario4dev.netlify.app/api/categories', { next: { revalidate: 3600 } })
    return res.json()
}
async function getProducts() {
    let res = await fetch("https://dario4dev.netlify.app/api/products", { next: { revalidate: 10 } })
    return res.json()
}
export default async function page(params) {
    const currentCategory = params.params.id;
    const availableCategory = await getCategories()
    const products = await getProducts()

    const filtredProduct = products.filter((product => product.category == currentCategory))
    let array = [];
    for (let i = 0; i < 4; i++) {
        array[i] = availableCategory[i].name

    }
    if (!array.includes(currentCategory)) {
        return <Error />
    }
    else {
        return (
            <div>
                <Dash />

                <div className='content  mt-24 pb-8'>

                    {filtredProduct.map((product => (

                        <Sellcard img={product.images[0]} id={product.id} key={product.id} productName={product.name} description={product.description} price={product.price} />
                    )))}
                </div>
            </div>
        )
    }
};