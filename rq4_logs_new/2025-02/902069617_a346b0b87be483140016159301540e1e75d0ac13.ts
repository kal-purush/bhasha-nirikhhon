import { useState, useEffect } from "react";

const PARCELS_KEY = "parcels";

interface Parcel {
  trackingNumber: string;
  idOrOtp: string;
  clientName: string;
  status: "completed" | "failed"; // Add status tracking
  timestamp: string;
}

const useParcelStorage = () => {
  const [parcels, setParcels] = useState<Parcel[]>([]);

  // Load parcels from local storage on mount
  useEffect(() => {
    const storedParcels = JSON.parse(localStorage.getItem(PARCELS_KEY) || "[]");
    setParcels(storedParcels);
  }, []);

  // Function to add a new parcel
  const addParcel = (trackingNumber: string, idOrOtp: string, clientName: string, status: "completed" | "failed") => {
    const newParcel: Parcel = {
      trackingNumber,
      idOrOtp,
      clientName,
      status,
      timestamp: new Date().toISOString(),
    };

    setParcels((prevParcels) => {
      const updatedParcels = [...prevParcels, newParcel];
      localStorage.setItem(PARCELS_KEY, JSON.stringify(updatedParcels));
      return updatedParcels;
    });
  };

  return { parcels, addParcel };
};

export default useParcelStorage;