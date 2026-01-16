import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './ModifyItemModal.css'; // Ensure you have relevant styles

const ModifyItemModal = ({ show, onClose, item, onItemModified }) => {
    const [modifiedItem, setModifiedItem] = useState(item);

    useEffect(() => {
        setModifiedItem(item);
    }, [item]);

    const handleChange = (e) => {
        const { name, value } = e.target;
        setModifiedItem(prevItem => ({
            ...prevItem,
            [name]: value
        }));
    };

    const handleSubmit = async (e) => {
        e.preventDefault();

        if (!modifiedItem || !modifiedItem.product_id) {
            console.error('Product ID is missing.');
            return;
        }

        try {
            await axios.put(`http://localhost:5000/api/products/${modifiedItem.product_id}`, modifiedItem);
            onItemModified(modifiedItem); // Notify parent component about the modification
            onClose(); // Close the modal
        } catch (error) {
            console.error('Error updating item:', error);
        }
    };

    if (!show) return null;

    return (
        <div className="modal-overlay">
            <div className="modal-content">
                <h2>Modify Item</h2>
                <form onSubmit={handleSubmit}>
                    <label>
                        Category:
                        <input
                            type="text"
                            name="category"
                            value={modifiedItem.category || ''}
                            onChange={handleChange}
                        />
                    </label>
                    <label>
                        Brand:
                        <input
                            type="text"
                            name="brand"
                            value={modifiedItem.brand || ''}
                            onChange={handleChange}
                        />
                    </label>
                    <label>
                        Item Name:
                        <input
                            type="text"
                            name="item_name"
                            value={modifiedItem.item_name || ''}
                            onChange={handleChange}
                        />
                    </label>
                    <label>
                        Item Size:
                        <input
                            type="text"
                            name="item_size"
                            value={modifiedItem.item_size || ''}
                            onChange={handleChange}
                        />
                    </label>
                    <label>
                        Description:
                        <textarea
                            name="description"
                            value={modifiedItem.description || ''}
                            onChange={handleChange}
                        />
                    </label>
                    <label>
                        Warehouse ID:
                        <input
                            type="number"
                            name="p_warehouse_id"
                            value={modifiedItem.p_warehouse_id || ''}
                            onChange={handleChange}
                        />
                    </label>
                    <label>
                        Price:
                        <input
                            type="number"
                            name="standard_price"
                            value={modifiedItem.standard_price || ''}
                            onChange={handleChange}
                        />
                    </label>
                    <button type="submit">Save</button>
                    <button type="button" onClick={onClose}>Close</button>
                </form>
            </div>
        </div>
    );
};

export default ModifyItemModal;