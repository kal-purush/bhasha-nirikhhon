import React, {useState, useEffect} from 'react'
import db from '../firebase';

const LinkForm = (props) => {

    const initialStateProducts = {
        producto: '',
        precio: '',
        descripcion: ''
    };

    const [products, setProducts] = useState(initialStateProducts);

    const handleInputChange = e => {
        const { name, value } = e.target;
        console.log(name, value)
        setProducts({
            ...products,
            [name]: value
        })
    };

    const handleSubmit = e => {
        e.preventDefault();
        props.addOrEditProduct(products);
        setProducts({...initialStateProducts})
    };

    const getProductById = async (id) => {
        const prod = await db.collection('productos').doc(id).get();
        setProducts({...prod.data()})
    };

    useEffect( () => {
        if(props.currentId === '') {
            setProducts({...initialStateProducts});
        } else {
            console.log(props.currentId)
            getProductById(props.currentId);
        }
    }, [props.currentId]);

    return (
        <form className="card card-body" onSubmit={handleSubmit}>
            <div className="form-group input-group">
                <div className="input-group-text bg-light">
                    <i className="material-icons">insert_link</i>
                </div>
                <input
                    type="text"
                    className="form-control"
                    placeholder="Producto"
                    name="producto"
                    value={products.producto}
                    onChange={handleInputChange}
                />
            </div>

            <div className="form-group input-group">
                <div className="input-group-text bg-light">
                    <i className="material-icons">create</i>
                </div>
                <input
                    type="text"
                    className="form-control"
                    placeholder="Precio"
                    name="precio"
                    value={products.precio}
                    onChange={handleInputChange}
                />
            </div>

            <div className="form-group">
                <textarea
                    name="descripcion"
                    rows="3"
                    className="form-control"
                    placeholder="Descripcion producto"
                    value={products.descripcion}
                    onChange={handleInputChange}>
                </textarea>
            </div>

            <button className="btn btn-primary btn-block">
                { props.currentId === '' ? 'Guardar' : 'Actualizar' }
            </button>
        </form>
    )
};

export default LinkForm;