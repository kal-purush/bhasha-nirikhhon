import React from "react";
import { useForm } from "react-hook-form";
import { useDispatch, useSelector } from "react-redux";
import { useHistory } from "react-router-dom";
import { fetchAddFriend } from "../Reducers/actions";

export default function FriendForm() {
  const history = useHistory();
  const dispatch = useDispatch();
  const logStatu = useSelector((store) => store.log);

  const {
    register,
    handleSubmit,
    reset,
    formState: { errors, isValid },
  } = useForm({
    mode: "onChange",
    defaultValues: { id: "", name: "", age: 0, email: "" },
  });

  function onSubmit(log) {
    let newFriend = {
      id: Math.random() * 100,
      name: log.name,
      age: Math.random() * 40 + 10,
      email: log.email,
    };
    dispatch(fetchAddFriend(newFriend));
    history.push("/friends");
  }

  return (
    <form onSubmit={handleSubmit(onSubmit)}>
      <div className="w-80 mx-auto mt-8">
        <label className="block ml-12 text-left mb-0" htmlFor="friendName">
          Friend Name
        </label>
        <br />
        <input
          className="px-8 py-4 bg-black mb-4 mt-0 rounded-md text-white border-b-1 border-black"
          id="friendName"
          name="friendName"
          type="text"
          {...register("name", {
            required: "İsim yazmalısınız",
            minLength: {
              value: 3,
              message: "İsim en az 3 karakter olmalı",
            },
          })}
        />
        {errors.name && (
          <p className="px-8 py-4 bg-red-500 mb-4 mt-0 rounded-md text-white border-b-1 border-red-900 w-30">
            {errors.name.message}
          </p>
        )}
      </div>
      <div className="w-80 mx-auto mt-4">
        <label className="block ml-12 text-left mb-0" htmlFor="email">
          Friend E-mail
        </label>
        <br />
        <input
          className="px-8 py-4 bg-black mb-4 mt-0 rounded-md text-white border-b-1 border-black"
          id="email"
          type="text"
          {...register("email", {
            validate: {
              passwordRequired: (p) =>
                p.length <= 10 || "En fazla 10 karakter girmelisiniz",
              minFive: (p) =>
                p.length >= 5 || "En az 5 karakter girmeniz gerekir",
            },
          })}
        />
        {errors.email && (
          <p className="px-8 py-4 bg-red-500 mb-4 mt-0 rounded-md text-white border-b-1 border-red-900 w-30">
            {errors.email.message}
          </p>
        )}
      </div>

      <button
        type="submit"
        disabled={!isValid}
        className="px-8 py-4 bg-black mb-4 mt-0 rounded-md text-white border-b-1 border-black"
      >
        Submit
      </button>
    </form>
  );
}