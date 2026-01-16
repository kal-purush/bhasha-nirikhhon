import React, { useRef, useState } from 'react';
import { useFormik } from 'formik';
import * as yup from 'yup';
import TextField from '@mui/material/TextField';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';
import Button from '@mui/material/Button';
import { InputLabel, Stack } from '@mui/material';
import emailjs from '@emailjs/browser';
import Swal from 'sweetalert2';

import {ClipLoader} from "react-spinners"


const validationSchema = yup.object({
    name: yup.string().required('Name is required'),
    email: yup.string().email('Invalid email format').required('Email is required'),
    phone: yup.number().required('Phone Number is'),
    vehicleType: yup.string().required('Vehicle type is required'),
    date: yup.date().required("Date is Required")
});

const initialValues = {
    name: '',
    email: '',
    phone: '',
    vehicleType: '',
    date: ''
};

const vehicleOptions = ['bike', 'activa', 'jupiter'];

const ContactForm = ({toast}) => {
    const [isLoading, setIsLoading] = useState(false)
    const formRef = useRef(null)
    const formik = useFormik({
        initialValues,
        validationSchema,
        onSubmit: (values) => {
            console.log(values);
            console.log("form ref", formRef.current);
            setIsLoading(true)
            Swal.fire({
                title: 'Are you sure?',
                text: "You Want to submit details !",
                icon: 'warning',
                showCancelButton: true,
                confirmButtonColor: '#3085d6',
                cancelButtonColor: '#d33',
                confirmButtonText: isLoading ? <><ClipLoader color="#36d7b7" /></> : 'Yes,send it!'
            }).then((result) => {
                if (result.isConfirmed) {
                  setTimeout(() => {
                    emailjs.sendForm('service_q2bcp6p', 'template_be0mzte', formRef.current, '9VucweErAbfIsKX78')
                    .then((result) => {
                        setIsLoading(false)
                        console.log(result.text);
                        toast.success("Your Details have been Send !")
                    }, (error) => {
                        console.log(error.text);
                        toast.error("details not send, Try Again")
                    });
                  }, 1500);
                }
            })

        },
    });
console.log("toast innn",toast);
    return (
        <Stack >
            <form onSubmit={formik.handleSubmit} ref={formRef} >
                <MenuItem>
                    <FormControl fullWidth>
                        <TextField
                            label="Name"
                            size='small'
                            id="name"
                            name="name"
                            variant="outlined"
                            {...formik.getFieldProps('name')}
                            error={formik.touched.name && Boolean(formik.errors.name)}
                            helperText={formik.touched.name && formik.errors.name}
                        />
                    </FormControl>
                </MenuItem>
                <MenuItem>
                    <FormControl fullWidth>
                        <TextField
                            label="Email"
                            size='small'
                            id="email"
                            name="email"
                            variant="outlined"
                            {...formik.getFieldProps('email')}
                            error={formik.touched.email && Boolean(formik.errors.email)}
                            helperText={formik.touched.email && formik.errors.email}
                        />
                    </FormControl>
                </MenuItem>
                <MenuItem>
                    <FormControl fullWidth>
                        <TextField
                            size='small'
                            label="Phone"
                            id="phone"
                            name="phone"
                            variant="outlined"
                            {...formik.getFieldProps('phone')}
                            error={formik.touched.phone && Boolean(formik.errors.phone)}
                            helperText={formik.touched.phone && formik.errors.phone}
                        />
                    </FormControl>
                </MenuItem>
                <MenuItem>
                    <FormControl fullWidth>
                        <InputLabel id="demo-simple-select-label">Type of Vehicle</InputLabel>
                        <Select
                            label="Type of Vehicle"
                            size='small'
                            id="vehicleType"
                            name="vehicleType"
                            variant="outlined"
                            value={formik.values.vehicleType}
                            onChange={formik.handleChange}
                            error={formik.touched.vehicleType && Boolean(formik.errors.vehicleType)}
                        >
                            {vehicleOptions.map((option) => (
                                <MenuItem key={option} value={option}>
                                    {option}
                                </MenuItem>
                            ))}
                        </Select>
                    </FormControl>
                </MenuItem>
                <MenuItem>
                    <FormControl fullWidth>
                        <TextField
                            label="On Date"
                            size='small'
                            id="date"
                            type='date'
                            name="date"
                            variant="outlined"
                            {...formik.getFieldProps('date')}
                            error={formik.touched.date && Boolean(formik.errors.date)}
                            helperText={formik.touched.date && formik.errors.date}
                        />
                    </FormControl>
                </MenuItem>
                <MenuItem sx={{ display: "flex", justifyContent: "center" }}>
                    <Button
                        variant="contained"
                        color="primary"
                        type="submit"
                        fullWidth
                        style={{ marginTop: '20px', backgroundColor: "blueviolet", fontSize: "12px" }}
                    >
                        { isLoading ? <><ClipLoader color="#36d7b7" /></> :  <>Send Request</>}
                       
                    </Button>
                </MenuItem>
            </form>
            
        </Stack>
    );
};

export default ContactForm;