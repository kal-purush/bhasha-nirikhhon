"use client"; // აუცილებელია

import { useState, useEffect } from "react";

const FetchCategories = () => {
  const [productData, setProductData] = useState<any[]>([]); // პროდუქციის მონაცემები
  const [loading, setLoading] = useState(true); // თუ დაველოდებით მონაცემების დატვირთვას
  const [error, setError] = useState<string | null>(null); // შეცდომა

  useEffect(() => {
    // აქ ხდება API-ს გაზრდა
    const fetchProductData = async () => {
      try {
        const response = await fetch(
          "https://geguchadzeadmin.pythonanywhere.com/products/subcategories/"
        ); // fakestoreapi.com-დან მონაცემების მიღება
        if (!response.ok) {
          throw new Error("Failed to fetch products");
        }
        const data = await response.json();
        setProductData(data); // აქ გსურთ მონაცემების დაწერა
      } catch (error) {
        setError("Failed to load products"); // თუ მოხდა შეცდომა
      } finally {
        setLoading(false); // დაიწია მონაცემების დატვირთვის პროცესი
      }
    };

    fetchProductData();
  }, []); // მხოლოდ ერთხელ დავამთავრებთ ამ პროცესს

  return { productData, loading, error }; // ვაბრუნებთ მონაცემებს
};

export default FetchCategories;