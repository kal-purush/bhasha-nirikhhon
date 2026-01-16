import React, { useState } from 'react';
import axios from 'axios';

const ItemForm = ({ refreshItems }) => {
    const [name, setName] = useState('');
    const [description, setDescription] = useState('');
    const [quantity, setQuantity] = useState('');
    const [price, setPrice] = useState('');
    const [errorMessage, setErrorMessage] = useState('');
    const [successMessage, setSuccessMessage] = useState('');

    const handleSubmit = async (e) => {
        e.preventDefault();
        setErrorMessage('');
        setSuccessMessage('');

        // Validation
        if (!name || !description || !quantity || !price) {
            setErrorMessage('All fields are required.');
            return;
        }

        if (parseInt(quantity, 10) <= 0 || parseFloat(price) <= 0) {
            setErrorMessage('Quantity and price must be greater than zero.');
            return;
        }

        try {
            // Make the POST request
            await axios.post('http://127.0.0.1:8080/api/items/', {
                name,
                description,
                quantity: parseInt(quantity, 10),
                price: parseFloat(price),
            });

            // Clear the form fields
            setName('');
            setDescription('');
            setQuantity('');
            setPrice('');

            // Refresh the items list
            refreshItems();

            // Show success message
            setSuccessMessage('Item added successfully!');
        } catch (error) {
            console.error('Error adding item:', error);
            setErrorMessage('Failed to add the item. Please try again.');
        }
    };

    return (
        <form className="mb-4" onSubmit={handleSubmit}>
            {errorMessage && (
                <div className="alert alert-danger" role="alert">
                    {errorMessage}
                </div>
            )}
            {successMessage && (
                <div className="alert alert-success" role="alert">
                    {successMessage}
                </div>
            )}
            <div className="form-group">
                <input
                    type="text"
                    className="form-control mb-2"
                    placeholder="Name"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                />
                <textarea
                    className="form-control mb-2"
                    placeholder="Description"
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                />
                <input
                    type="number"
                    className="form-control mb-2"
                    placeholder="Quantity"
                    value={quantity}
                    onChange={(e) => setQuantity(e.target.value)}
                />
                <input
                    type="number"
                    className="form-control mb-2"
                    placeholder="Price"
                    value={price}
                    onChange={(e) => setPrice(e.target.value)}
                />
                <button type="submit" className="btn btn-primary">
                    Add Item
                </button>
            </div>
        </form>
    );
};

export default ItemForm;