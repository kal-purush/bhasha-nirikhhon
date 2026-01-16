import { useState, useEffect } from "react";
import axios from "axios";
import jwtDecode from "jwt-decode";
import { Pet } from "../types/PetTypes"; // Assuming a Pet interface exists

export const usePets = () => {
  const [pets, setPets] = useState<Pet[]>([]);
  const [loading, setLoading] = useState<boolean>(false); // To track loading state
  const [error, setError] = useState<string>(""); // To track error messages
  const token = localStorage.getItem("token");

  // Helper function to extract user_id from the token
  const getUserIdFromToken = (token: string): string | null => {
    try {
      const decodedToken: any = jwtDecode(token);
      return decodedToken.user_id || null; // Adjust based on your token structure
    } catch (error) {
      console.error("Failed to decode token:", error);
      return null;
    }
  };

  // Fetch Pets
  const fetchPets = async () => {
    setLoading(true); // Start loading
    setError(""); // Clear previous errors

    try {
      const response = await axios.get("http://localhost:3002/pets/getPets", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      console.log("Fetched pets:", response.data);

      const petsWithDateTime = response.data.map((pet: any) => ({
        ...pet,
      }));

      console.log("Mapped pets:", petsWithDateTime);

      setPets(petsWithDateTime);
    } catch (error: any) {
      console.error("Error fetching pets:", error);
      setError("Failed to fetch pets. Please try again later.");
    } finally {
      setLoading(false); // End loading
    }
  };

  useEffect(() => {
    fetchPets(); // Fetch pets on mount
  }, []);

  // Add Pet
  const addPet = async (newPet: Pet) => {
    if (!token) {
      console.error("User not authenticated. No token found.");
      setError("User not authenticated. No token found.");
      return;
    }

    try {
      // Get user_id from the authenticated session
      const userId = getUserIdFromToken(token);

      if (!userId) {
        console.error("Failed to retrieve user_id.");
        setError("Failed to retrieve user details. Please log in again.");
        return;
      }

      // Include user_id in the payload
      const petWithUserId = {
        ...newPet,
        user_id: userId,
      };

      console.log("Inserting pet data:", petWithUserId);

      await axios.post("http://localhost:3002/pets/insertPet", petWithUserId, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      fetchPets(); // Refresh the pet list after adding
    } catch (error: any) {
      console.error("Error adding pet:", error);
      setError("Failed to add pet. Please try again later.");
    }
  };

  // Save Pet (Update Pet)
  const savePet = async (
    updatedPet: Pet,
    currentPets: Pet[],
    editingPetId: string
  ) => {
    if (!token) {
      console.error("User not authenticated. No token found.");
      setError("User not authenticated. No token found.");
      return;
    }

    try {
      await axios.patch(
        `http://localhost:3002/pets/updatePet/${editingPetId}`,
        updatedPet,
        {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );
      setPets(currentPets); // Update the pets list after the save
    } catch (error: any) {
      console.error("Error saving pet:", error);
      setError("Failed to save pet. Please try again later.");
    }
  };

  // Delete Pet
  const deletePet = async (pet_id: string) => {
    setPets((prevPets) => prevPets.filter((pet) => pet.pet_id !== pet_id));

    try {
      await axios.delete(`http://localhost:3002/pets/deletePet/${pet_id}`, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
    } catch (error: any) {
      console.error("Error deleting pet:", error);
      setError("Failed to delete pet. Please try again later.");
    }
  };

  return {
    fetchPets,
    addPet,
    pets,
    setPets,
    savePet,
    deletePet,
    loading,
    error, // Return loading and error states
  };
};