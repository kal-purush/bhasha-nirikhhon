import React from "react";
import InputVal from "../utility/InputVal";
import { errNotify } from "../utility/notify";
import { ToastContainer } from "react-toastify";
import { useForm } from "react-hook-form";

const UpdateForm = ({ data }) => {
  const { register, handleSubmit } = useForm();

  const onSubmit = () => {
    errNotify("hello");
  };

  const handleChange = (e) => {
    console.log(e);
  };
  return (
    <form className="" onSubmit={handleSubmit(onSubmit)}>
      <ToastContainer />
      <InputVal
        placeholder={"Business Name"}
        type={"text"}
        value={data.businessName}
        register={{ ...register("owner", { required: true }) }}
        onChange={handleChange}
      />

      <InputVal
        placeholder={"Owner Name"}
        type={"text"}
        value={data.ownerName}
        register={null}
        onChange={handleChange}
      />
    </form>
  );
};

export default UpdateForm;