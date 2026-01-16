import React from 'react'
import { useState } from 'react';
import Header from './Header'
import axios from 'axios';

export default function SellBook() {

    const [image, setImage] = useState("")

    const uploadImage = async (e) => {

        const imageFile = e.target.files[0];
        const formData = new FormData();
        formData.append("file", imageFile);
        formData.append("upload_preset", "tatvasoftImages");

        const res = await fetch("https://api.cloudinary.com/v1_1/tatvasoft/image/upload",
            {
                method: "POST",
                body: formData
            })

        const fileDetails = await res.json()
        console.log(fileDetails.url)

        setImage(fileDetails.url)
    }

    const addToDatabase = async (e) => {
        e.preventDefault()

        const title = e.target.elements.title.value;
        const discount = parseInt(e.target.elements.discount.value);
        const price = (e.target.elements.price.value);
        const imageUrl = image + ""
        const description = e.target.elements.description.value;

        const bodyData = {
            "title": title,
            "discount": discount,
            "price": price,
            "image": imageUrl,
            "description": description
        }


        axios
            .post("https://rest-api-tatvasoft.herokuapp.com/api/post", bodyData)
            .then(res => console.log(res))
            .catch(err => console.log(err));
    }


    return (
        <div>
            <Header />
            <div className='container'>
                <form onSubmit={addToDatabase}>
                    <h1 className='my-5'>Sell your book here !!</h1>
                    <div className="d-flex flex-row mt-4 row justify-content-between">
                        <div className="d-flex flex-row col-sm-4 justify-content-between align-items-center">
                            <label>Title*</label>&nbsp;&nbsp;
                            <input type="text" name='title' size="16" className='form-control'></input>
                        </div>
                        <div className="d-flex flex-row col-sm-4 justify-content-between align-items-center">
                            <label>Price*</label>&nbsp;&nbsp;
                            <input type="text" name="price" size="16" className='form-control'></input>
                        </div>
                    </div>

                    <div className="d-flex flex-row mt-4 row justify-content-between">
                        <div className="d-flex flex-row col-sm-4 justify-content-between align-items-center">
                            <label>Sale%*</label>&nbsp;&nbsp;
                            <input type="text" name="discount" size="16" className='form-control'></input>
                        </div>
                        <div className="d-flex flex-row col-sm-4 justify-content-between align-items-center">
                            <label>Image*</label>&nbsp;&nbsp;
                            <input type="file" name="Image" size="16" className='form-control' onChange={uploadImage}></input>
                        </div>
                    </div>

                    <div className="my-5 ">
                        <label className='d-block'>Description:</label>&nbsp;&nbsp;
                        <textarea className='col-sm-12' name="description"></textarea>

                    </div>
                    <button className='btn btn-success px-5' type='submit'>Sell Book</button>
                </form>

                <img src={image} />
            </div>
        </div >
    )

}