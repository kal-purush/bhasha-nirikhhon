import axios from '../../../axios'
import { Field, Form, Formik } from 'formik'
import React, { useRef, useState } from 'react'
import toast from 'react-hot-toast'
import Modal from 'react-modal'
import * as yup from 'yup';
import FieldError from '../../../components/FieldError'
import Select from 'react-select'

function AddVehicle({ modalIsOpen, closeModal, getRoute }) {

    const [images, setImages] = useState([])
    const uploadRef = useRef(null)

    console.log('images', images)

    const handleImages = (files) => {

        console.log('files', files)
        setImages([
            ...files
        ])
    }

    const handleButtonClick = () => {
        // Access and interact with the DOM element
        if (uploadRef.current) {
            uploadRef.current.click()
        }
    }

    const handleFormSubmit = async (values, actions) => {
        try {
            let formData = new FormData

            for (let value in values) {
                if (typeof values[value] === "object") {
                    formData.append(value, JSON.stringify(values[value]))
                } else formData.append(value, values[value])
            }

            for (let image in images) {
                formData.append('images', images[image])
            }


            let result = await axios.post('/vehicle', formData)

            if (result.data.success) {
                toast.success('Vehicle Added Successfully')
                closeModal()
                getRoute()
            } else toast.error('Failed')
        } catch (ERR) {
            console.log(ERR)
            toast.error(ERR.response.data.message)
        }
    }

    const validationSchema = yup.object({
        name: yup.string()
            .required('This Field is required'),
        description: yup.string()
            .required('This Field is required'),
        seat: yup.string()
            .required('This Field is required'),
        type: yup.string()
            .required('This Field is required'),
        price: yup.string()
            .required('This Field is required'),
        engine: yup.string()
            .required('This Field is required'),
        year: yup.string()
            .required('This Field is required'),
        model: yup.string()
            .required('This Field is required'),
        mileage: yup.string()
            .required('This Field is required'),
        fuel_type: yup.string()
            .required('This Field is required'),

    });

    const removeItem = async (index) => {
        try {
            const tempImgs = [...images]

            tempImgs.splice(index, 1)

            setImages(tempImgs)
        } catch (ERR) {
            console.log(ERR)
            toast.error(ERR.response.data.msg)
        }
    }

    return (
        <Modal
            ariaHideApp={false}
            isOpen={modalIsOpen}
            onRequestClose={closeModal}
            contentLabel="Add Vehicle Modal"
            overlayClassName="Overlay"
            className="Modal rounded-md p-5 md:w-3/4 max-h-screen overflow-auto"
        >
            <h1 className="text-4xl font-bold tracking-tight text-gray-900">Add Vehicle</h1>

            <div className='mt-4'>
                <Formik
                    enableReinitialize
                    initialValues={{
                        name: "",
                        description: "",
                        type: "",
                        price: "",
                        engine: "",
                        year: "",
                        model: "",
                        mileage: "",
                        fuel_type: "",
                    }}
                    validationSchema={validationSchema}
                    onSubmit={(values, actions) => {
                        handleFormSubmit(values, actions);
                    }}
                >
                    {(props) => (
                        <Form className='gap-3 grid grid-cols-2'>
                            <div className=''>
                                <label htmlFor="name" className="block text-sm font-medium leading-6 text-gray-900">
                                    Vehicle Name
                                </label>
                                <div className="mt-2">
                                    <Field
                                        id="name"
                                        name="name"
                                        autoComplete="name"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    />
                                </div>
                                <FieldError message={props.touched.name && props.errors.name} />

                            </div>
                            <div className=''>
                                <label htmlFor="year" className="block text-sm font-medium leading-6 text-gray-900">
                                    Year
                                </label>
                                <div className="mt-2">
                                    <Field
                                        id="year"
                                        name="year"
                                        autoComplete="year"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    />
                                </div>
                                <FieldError message={props.touched.year && props.errors.year} />
                            </div>

                            <div className='col-span-full'>
                                <label htmlFor="description" className="block text-sm font-medium leading-6 text-gray-900">
                                    Vehicle Description
                                </label>
                                <div className="mt-2">
                                    <Field
                                        as="textarea"
                                        id="description"
                                        name="description"
                                        autoComplete="description"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    />
                                </div>
                                <FieldError message={props.touched.description && props.errors.description} />
                            </div>

                            <div className=''>
                                <label htmlFor="type" className="block text-sm font-medium leading-6 text-gray-900">
                                    Type
                                </label>
                                <div className="mt-2">
                                    <Field
                                        as="select"
                                        id="type"
                                        name="type"
                                        autoComplete="type"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    >
                                        <option value={""}>Please Select a Type</option>
                                        <option value={"two-wheeler"}>Two-Wheeler</option>
                                        <option value={"four-wheeler"}>Four-Wheeler</option>

                                    </Field>
                                </div>
                                <FieldError message={props.touched.type && props.errors.type} />
                            </div>
                            <div className=''>
                                <label htmlFor="fuel_type" className="block text-sm font-medium leading-6 text-gray-900">
                                    Fuel Type
                                </label>
                                <div className="mt-2">
                                    <Field
                                        as="select"
                                        id="fuel_type"
                                        name="fuel_type"
                                        autoComplete="fuel_type"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    >
                                        <option value={""}>Please Select a Fuel Type</option>
                                        <option value={"diesel"}>Diesel</option>
                                        <option value={"petrol"}>Petrol </option>
                                        <option value={"electric"}>Electric</option>

                                    </Field>
                                </div>
                                <FieldError message={props.touched.fuel_type && props.errors.fuel_type} />
                            </div>
                            <div className=''>
                                <label htmlFor="price" className="block text-sm font-medium leading-6 text-gray-900">
                                    Price per Day
                                </label>
                                <div className="mt-2">
                                    <Field
                                        min={1}
                                        type="number"
                                        id="price"
                                        name="price"
                                        autoComplete="price"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    />
                                </div>
                                <FieldError message={props.touched.price && props.errors.price} />
                            </div>
                            <div className=''>
                                <label htmlFor="seat" className="block text-sm font-medium leading-6 text-gray-900">
                                    Seat
                                </label>
                                <div className="mt-2">
                                    <Field
                                        min={1}
                                        type="number"
                                        id="seat"
                                        name="seat"
                                        autoComplete="seat"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    />
                                </div>
                                <FieldError message={props.touched.seat && props.errors.seat} />
                            </div>
                            <div className=''>
                                <label htmlFor="engine" className="block text-sm font-medium leading-6 text-gray-900">
                                    Engine
                                </label>
                                <div className="mt-2">
                                    <Field
                                        id="engine"
                                        name="engine"
                                        autoComplete="engine"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    />
                                </div>
                                <FieldError message={props.touched.engine && props.errors.engine} />
                            </div>
                            <div className=''>
                                <label htmlFor="model" className="block text-sm font-medium leading-6 text-gray-900">
                                    Model
                                </label>
                                <div className="mt-2">
                                    <Field
                                        id="model"
                                        name="model"
                                        autoComplete="model"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    />
                                </div>
                                <FieldError message={props.touched.model && props.errors.model} />
                            </div>
                            <div className=''>
                                <label htmlFor="mileage" className="block text-sm font-medium leading-6 text-gray-900">
                                    Mileage
                                </label>
                                <div className="mt-2">
                                    <Field
                                        id="mileage"
                                        name="mileage"
                                        autoComplete="mileage"
                                        required
                                        className="block w-full rounded-md border-0 py-1.5 px-2 text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                                    />
                                </div>
                                <FieldError message={props.touched.mileage && props.errors.mileage} />
                            </div>

                            <div className=''>
                                <span className='font-semibold'>Image</span>
                                <div className='mt-2'>
                                    <input
                                        multiple
                                        ref={uploadRef}
                                        type='file'
                                        accept="image/png, image/jpeg"
                                        onChange={(e) => handleImages(e.target.files)}
                                        className='inputfield hidden'
                                        name='image'
                                        id='image'
                                        placeholder='End Date'
                                    />
                                </div>
                                <button
                                    type='button'
                                    onClick={() => {
                                        handleButtonClick()
                                    }}
                                    className='border-2 border-dashed w-full h-20'
                                >
                                    Upload
                                </button>
                                <div className='flex gap-2 items-center mt-3'>
                                    {
                                        images.length > 0 && images?.map((value, index) => (
                                            <div className='relative'>
                                                <button type='button' onClick={() => {
                                                    removeItem(index)
                                                }} className='absolute bottom-0 bg-red-600 w-full text-white text-xs'>Remove</button>
                                                <img src={URL.createObjectURL(value)} key={index} alt="prod_img" className='h-24 w-24 object-contain border shadow' />
                                            </div>
                                        ))
                                    }

                                </div>
                            </div>

                            <div className='col-span-full text-center opacity-15'>
                                ------------
                            </div>

                            <div className='col-span-full mt-4'>
                                <button
                                    type="submit"
                                    className="flex w-full justify-center rounded-md bg-indigo-600 px-3 py-1.5 text-sm font-semibold leading-6 text-white shadow-sm hover:bg-indigo-500 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-indigo-600"
                                >
                                    Register
                                </button>
                            </div>


                        </Form>
                    )}
                </Formik>
            </div>

        </Modal>
    )
}

export default AddVehicle