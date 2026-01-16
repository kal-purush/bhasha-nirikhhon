import React, { useState } from "react";
import FieldForm from "./FieldForm";
import ExistingFields from "./ExistingFields";

const AdminPanel = ({ fields, onFieldsChange }) => {
  const [newField, setNewField] = useState({
    type: "text",
    label: "",
    name: "",
    required: false,
    options: [],
    subtype: "general",
  });

  const [radioOptions, setRadioOptions] = useState([]);
  const [checkboxOptions, setCheckboxOptions] = useState([]);

  const handleNewFieldChange = (event) => {
    const { name, value, type, checked } = event.target;
    const newValue = type === "checkbox" ? checked : value;

    if (name === "options") {
      const optionsArray = value.split(",").map((option) => ({
        label: option.trim(),
        value: option.trim(),
      }));
      setNewField((prev) => ({ ...prev, options: optionsArray }));
    } else {
      setNewField((prev) => ({ ...prev, [name]: newValue }));
    }
  };

  const handleAddField = () => {
    if (newField.label && newField.name) {
      const updatedFields = [...fields, newField];
      onFieldsChange(updatedFields);
      setNewField({
        type: "text",
        label: "",
        name: "",
        required: false,
        options: [],
        subtype: "general",
      });
      setRadioOptions([]);
      setCheckboxOptions([]);
    } else {
      alert("Field label and name are required!");
    }
  };

  const handleRemoveField = (fieldName) => {
    const updatedFields = fields.filter((field) => field.name !== fieldName);
    onFieldsChange(updatedFields);
  };

  return (
    <section className="flex flex-col justify-center items-center">
    <h2 className="text-big ">Admin Panel</h2>
    <p className="text-small italic opacity-60">Here you can add or remove fields</p>
    <div className="w-[90%] md:w-[45%] mx-auto bg-[#181724] rounded-[1cqw] border-[0.05cqw] border-opacity-40 border-gray-300 px-[6cqw] py-[4cqw] md:px-[2cqw] md:py-[1.8cqw] mt-[8cqw] md:mt-[1cqw]">
      
      <FieldForm
        newField={newField}
        setNewField={setNewField} 
        radioOptions={radioOptions}
        setRadioOptions={setRadioOptions}
        checkboxOptions={checkboxOptions}
        setCheckboxOptions={setCheckboxOptions}
        handleNewFieldChange={handleNewFieldChange}
        handleAddField={handleAddField}
      />
      <div className="bg-gray-300 h-[0.8cqw] md:h-[0.05cqw] w-full my-[4cqw] md:my-[1cqw] opacity-50"></div>
      <ExistingFields fields={fields} handleRemoveField={handleRemoveField} />
    </div>
    </section>
  );
};

export default AdminPanel;