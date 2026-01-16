import React, { useState } from 'react';
import axios from 'axios';

function DepositForm() {
    const [formData, setFormData] = useState({
        Name: '',
        Email: '',
        Amount: ''
    });

    const handleChange = (e) => {
        setFormData({
            ...formData,
            [e.target.name]: e.target.value
        });
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        try {
            const response = await axios.post('http://localhost:8000/rolebased/deposit/', formData);
            console.log(response.data);
            // Additional logic after successful post (e.g., clear form, show message)
        } catch (error) {
            console.error("Error posting data: ", error);
            // Error handling logic
        }
    };

    return (
        <form onSubmit={handleSubmit}>
            <div>
                <label>Name</label>
                <input
                    type="text"
                    name="Name"
                    value={formData.Name}
                    onChange={handleChange}
                />
            </div>
            <div>
                <label>Email</label>
                <input
                    type="email"
                    name="Email"
                    value={formData.Email}
                    onChange={handleChange}
                />
            </div>
            <div>
                <label>Amount</label>
                <input
                    type="text"
                    name="Amount"
                    value={formData.Amount}
                    onChange={handleChange}
                />
            </div>
            <button type="submit">Submit</button>
        </form>
    );
}

export default DepositForm;