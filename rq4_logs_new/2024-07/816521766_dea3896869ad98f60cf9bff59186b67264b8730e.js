// InvoiceForm.js
import React, { useState } from "react";
import axios from "axios";

const InvoiceForm = () => {
  const [formData, setFormData] = useState({
    customerEmail: "",
    amount: "",
    description: "",
  });

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData({
      ...formData,
      [name]: value,
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      await axios.post(
        "http://docker-tutorial-env.eba-w5dxfa4t.us-east-2.elasticbeanstalk.com:3002/invoices",
        formData
      );
      alert("Factura creada y enviada con éxito");
    } catch (error) {
      console.error("Error al crear la factura", error);
      alert("Error al crear la factura");
    }
  };

  return (
    <form onSubmit={handleSubmit}>
      <label>
        Correo del Cliente:
        <input
          type="email"
          name="customerEmail"
          value={formData.customerEmail}
          onChange={handleChange}
          required
        />
      </label>
      <label>
        Monto:
        <input
          type="number"
          name="amount"
          value={formData.amount}
          onChange={handleChange}
          required
        />
      </label>
      <label>
        Descripción:
        <textarea
          name="description"
          value={formData.description}
          onChange={handleChange}
          required
        />
      </label>
      <button type="submit">Crear Factura</button>
    </form>
  );
};

export default InvoiceForm;