// Method 1: Using the useFormik hook

import React, {useEffect} from "react";
import {useFormik, ErrorMessage, Field} from "formik";
import * as Yup from "yup";

function BaseFormik() {
    const {getFieldProps, handleSubmit, errors, touched} = useFormik({
        initialValues: {
            email: "",
            password: ""
        },
        validationSchema: Yup.object().shape({
            email: Yup.string()
                .email("Invalid email address")
                .required("Required"),
            password: Yup.string()
                .min(8, "Must be more than 8 characters")
                .required("Required")
        }),
        onSubmit(values) {
            console.log(values);
        }
    });

    return (
        // We bind "onSubmit" to "formik.handleSubmit"
        <form className="baseForm" onSubmit={handleSubmit} noValidate>

            <div className="formFieldWrapInner">
                <input
                    type="email"
                    name="email"
                    id="email"
                    className="email formField"
                    {...getFieldProps("email")} // We pass the name of the dependent field

                />
            </div>

            <input
                type="text"
                name="password"
                id="password"
                className="password formField"
                {...getFieldProps("password")} // We pass the name of the dependent field

            />
            <button type="submit">
                Submit
            </button>
        </form>
    )
}

export default BaseFormik;