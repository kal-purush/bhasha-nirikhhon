/* eslint-disable react-hooks/exhaustive-deps */
// hooks/useRoomForm.ts
import { useState, useEffect } from "react";
import {
  RoomFormData,
  RoomDropdownData,
  RoomFormTabs,
} from "../types/room.types";
import { roomService } from "@/services/rooms/room.service";
import { RoomStatus } from "@/enums/room.enums";

export const useRoomForm = () => {
  const [activeTab, setActiveTab] = useState("basic");
  const [loading, setLoading] = useState(true);
  const [isSubmitting, setisSubmitting] = useState(false);
  const [formData, setFormData] = useState<RoomFormData>({
    name: "",
    type: "",
    bedType: "",
    numBeds: "1",
    size: "",
    maxGuests: "2",
    available: "1",
    basePrice: "",
    description: "",
    refundable: true,
    prepayment: false,
    breakfast: false,
    selectedAmenities: [],
    images: [],
    totalOccupancy: "2",
    limitAdults: false,
    maxAdults: "2",
    limitChildren: false,
    maxChildren: "0",
    numBathrooms: "1",
    bathrooms: [{ isPrivate: true, isInRoom: true }],
    roomStatus: RoomStatus.ACTIVE,
  });

  const [tabsEnabled, setTabsEnabled] = useState<RoomFormTabs>({
    basic: true,
    details: false,
    occupancy: false,
    amenities: false,
    images: false,
    pricing: false,
  });

  const [dropdownData, setDropdownData] = useState<RoomDropdownData>({
    roomTypes: [],
    bedTypes: [],
    amenities: [],
    privacyPolicies: [],
  });

  const validateTab = (tab: string) => {
    console.log(tab);

    switch (tab) {
      case "basic":
        return (
          formData.name.trim() !== "" &&
          formData.type !== "" &&
          formData.bedType !== "" &&
          formData.numBeds !== "" &&
          formData.maxGuests !== ""
        );
      case "details":
        return (
          formData.size !== "" &&
          formData.available !== "" &&
          formData.description.trim() !== "" &&
          formData.numBathrooms !== ""
        );
      case "occupancy":
        return formData.totalOccupancy !== "";
      case "amenities":
        return true;
      case "images":
        // return formData.images.length >= 3;
        return true;
      case "pricing":
        return formData.basePrice !== "";
      default:
        return false;
    }
  };

  const handleInputChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    setFormData((prev) => ({ ...prev, [name]: value }));
  };

  const handleSelectChange = (name: string, value: string) => {
    setFormData((prev) => ({ ...prev, [name]: value }));
  };

  const handleSwitchChange = (name: string, checked: boolean) => {
    setFormData((prev) => ({ ...prev, [name]: checked }));
  };

  const handleAmenityChange = (amenityId: string, checked: boolean) => {
    setFormData((prev) => ({
      ...prev,
      selectedAmenities: checked
        ? [...prev.selectedAmenities, amenityId]
        : prev.selectedAmenities.filter((id) => id !== amenityId),
    }));
  };

  const handleImagesChange = (images: string[]) => {
    setFormData((prev) => ({ ...prev, images }));
  };

  const nextTab = () => {
    console.log("asdasd: ", activeTab);
    if (!validateTab(activeTab)) return;
    switch (activeTab) {
      case "basic":
        setActiveTab("details");
        break;
      case "details":
        setActiveTab("occupancy");
        break;
      case "occupancy":
        setActiveTab("amenities");
        break;
      case "amenities":
        setActiveTab("images");
        break;
      case "images":
        setActiveTab("pricing");
        break;
    }
  };

  const prevTab = () => {
    switch (activeTab) {
      case "details":
        setActiveTab("basic");
        break;
      case "occupancy":
        setActiveTab("details");
        break;
      case "amenities":
        setActiveTab("occupancy");
        break;
      case "images":
        setActiveTab("amenities");
        break;
      case "pricing":
        setActiveTab("images");
        break;
    }
  };

  const handleCheckboxChange = (name: string, checked: boolean) => {
    setFormData((prev) => ({ ...prev, [name]: checked }));
  };

  const handleBathroomChange = (
    index: number,
    field: string,
    value: boolean
  ) => {
    setFormData((prev) => {
      const updatedBathrooms = [...prev.bathrooms];
      updatedBathrooms[index] = { ...updatedBathrooms[index], [field]: value };
      return { ...prev, bathrooms: updatedBathrooms };
    });
  };

  const handleNumBathroomsChange = (value: string) => {
    const numBathrooms = Number.parseInt(value);
    setFormData((prev) => {
      let bathrooms = [...prev.bathrooms];
      while (bathrooms.length < numBathrooms) {
        bathrooms.push({ isPrivate: true, isInRoom: true });
      }
      if (bathrooms.length > numBathrooms) {
        bathrooms = bathrooms.slice(0, numBathrooms);
      }
      return { ...prev, numBathrooms: value, bathrooms };
    });
  };

  useEffect(() => {
    const currentTabValid = validateTab(activeTab);
    setTabsEnabled((prev) => ({
      ...prev,
      [activeTab]: true,
      details: activeTab === "basic" ? currentTabValid : prev.details,
      occupancy: activeTab === "details" ? currentTabValid : prev.occupancy,
      amenities: activeTab === "occupancy" ? currentTabValid : prev.amenities,
      images: activeTab === "amenities" ? currentTabValid : prev.images,
      pricing: activeTab === "images" ? currentTabValid : prev.pricing,
    }));
  }, [formData, activeTab]);

  useEffect(() => {
    const fetchDropdowns = async () => {
      try {
        const data = await roomService.getDropdownData();
        setDropdownData(data.data);
      } catch (error) {
        console.error("Error fetching dropdown data: ", error);
      } finally {
        setLoading(false);
      }
    };
    fetchDropdowns();
  }, []);

  return {
    activeTab,
    setActiveTab,
    loading,
    isSubmitting,
    setisSubmitting,
    formData,
    setFormData,
    tabsEnabled,
    dropdownData,
    handleInputChange,
    handleSelectChange,
    handleSwitchChange,
    handleAmenityChange,
    handleImagesChange,
    nextTab,
    prevTab,
    isLastTab: activeTab === "pricing",
    isFirstTab: activeTab === "basic",
    validateTab,
    handleCheckboxChange,
    handleBathroomChange,
    handleNumBathroomsChange,
    setTabsEnabled,
  };
};