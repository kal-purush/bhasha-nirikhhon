import { useUserContext } from 'globalComponents/AppContext';
import majorCities from 'globalComponents/data/majorCities';
import React, { useEffect, useState } from 'react'
import { ThreeDots } from 'react-loader-spinner';
import { useLocation } from 'react-router-dom'
import axiosInstance from 'utils/AxiosInstance';
import { FaUser } from "react-icons/fa";

const UserSearchWindow = () => {
    const location = useLocation()
    const userData = location.state?.userData;

    const { setAlert, fetchUsers } = useUserContext()
    const [ editable, setEditable ] = useState(true)
    const [ changedDetails, setChangedDetails ] = useState(userData)
    const [ loader, setLoader ] = useState(false)
    const [ selectedCity, setSelectedCity ] = useState()
    // const [ PartnerDetails, setPartnersDetails ] = useState()

    function findChanges(obj1, obj2) {
        const result = {};
      
        for (const key in obj2) {
            if (obj2[key] !== obj1[key]) {
                result[key] = obj2[key]; // Include only changed values from obj2
            }
        }
      
        return result;
    }

    const handleFetchDriverDetails = (id) => {
        if (typeof id === "number") {
          setLoader(true)
          axiosInstance
            .get(`/account/users/${id}/`)
            .then(res => {
                const data = res.data;
                setChangedDetails(data)
            })
            .catch(err => {
                console.error(err)
            })
            .finally(() => {
                setLoader(false)
            })
        }
    }

    useEffect(() => {
        if (userData.user_type === "driver") {
            handleFetchDriverDetails(userData.id)
        }
    }, [userData])

    useEffect(() => {
        if(changedDetails) {
            setSelectedCity(changedDetails.area)
        }
    }, [changedDetails])
    
    // Handle input change
    const handleChange = (e) => {
        const { name, value } = e.target;
        
        // Update nested objects
        if (name.includes('driver_additional_details')) {
            const namesArray = name.split('.');
            const key = namesArray[namesArray.length-1]
            console.log(key, value)
            setChangedDetails(prev => ({
                ...prev,
                driver_additional_details: {
                    ...prev.driver_additional_details,
                    [key]: value
                }
            }));
        } else if (name.includes('another_additional_details')) {
            const namesArray = name.split('.');
            const key = namesArray[namesArray.length-1]
            console.log(key, value)
            setChangedDetails(prev => ({
                ...prev,
                another_additional_details: {
                    ...prev.another_additional_details,
                    [key]: value
                }
            }));
        } else {
        setChangedDetails({
            ...changedDetails,
            [name]: value
        });
        }
    };

    const handleSubmit = () => {
        setLoader(true)
        const data = findChanges(userData, changedDetails)

        axiosInstance
        .patch(`/all/consumer/${userData.id}/`, data)
        .then(res => {
            console.log(res)
            fetchUsers()
            // setDriverDetails(changedDetails)
            setAlert("Changed Successfully")
        })
        .catch(err => {
            console.error(err)
        })
        .finally(() => {
            setLoader(false)
            setEditable(false)
        })
    }

    const handleCityChange = (event) => {
        setSelectedCity(event.target.value)
        setChangedDetails(prev => ({
            ...prev,
            area: event.target.value,
        }));
    };

    const handleAreaChange = (event) => {
        setChangedDetails(prev => ({
            ...prev,
            city: event.target.value,
        }));
    };

    useEffect(() => {
        if (!editable) {
            setChangedDetails(userData)
        }
    }, [editable, userData])

    // const totalPercentage = (data) => { 
    //     // const total = data.reduce((sum, item) => sum + parseFloat(item.percentage), 0);
    
    //     // return total
    //   }

    //   const handlePartnerSubmit = () => {
    //     if (totalPercentage(changedDetails) !== 15) {
    //       setAlert("Total share of a franchise should be 15%")
    //       return
    //     }
    
    //     setLoader(true)
    
    //     axiosInstance
    //       .patch(`https://tooki.repsoft.in/all/editfranchisedetail/${userData.id}/editpartner/`, changedDetails)
    //       .then(res => {
    //         console.log(res)
    //         // fetchFranchise()
    //         setAlert("Changed successfully")
    //         // setEditable(prev => !prev)
    //         setPartnersDetails(changedDetails)
    //       })
    //       .catch(err => {
    //         console.error(err)
    //         setAlert("Something went wrong")
    //       })
    //       .finally(() => {
    //         setLoader(false)
    //       })
    //   }

  return (
    <div className='w-full min-h-full backdrop-blur-sm p-8 flex flex-col items-start justify-start'>
            {/* {JSON.stringify(userData)} */}
            {console.log(changedDetails)}

            {loader && 
            <div className='w-full h-full absolute top-0 left-0 bg-white z-30'>
                hello
            </div>}

        <div className='flex items-center justify-start gap-5 mb-5 ml-4'>
            <p className='text-3xl text-black font-semibold capitalize'>
                {userData.first_name + " " + userData.last_name}
            </p>

            <div className='bg-orange-600 text-white px-6 py-1 text-sm rounded-full capitalize font-medium'>
                {changedDetails.user_type}
            </div>
        </div>

        <div className='flex items-start justify-between gap-6 w-full pr-10'>
            <div className='max-w-[50rem] w-full flex flex-row flex-wrap items-start justify-start gap-y-6'>
                <div className='w-1/2 flex flex-col justify-start items-start px-2'>
                    <label className='text-sm text-orange-600 font-medium mb-1 ml-3'>First Name: </label>
                    <input
                        type="text"
                        className='w-full h-10 rounded-full border-2 border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4 focus:outline-0'
                        name="first_name" 
                        value={changedDetails.first_name} 
                        onChange={handleChange} 
                    />
                </div>
                <div className='w-1/2 flex flex-col justify-start items-start px-2'>
                    <label className='text-sm text-orange-600 font-medium mb-1 ml-3'>Last Name: </label>
                    <input
                        type="text"
                        className='w-full h-10 rounded-full border-2 border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4 focus:outline-0' 
                        name="last_name" 
                        value={changedDetails.last_name} 
                        onChange={handleChange} 
                    />
                </div>
                <div className='w-1/2 flex flex-col justify-start items-start px-2'>
                    <label className='text-sm text-orange-600 font-medium mb-1 ml-3'>Email: </label>
                    <input
                        type="email"
                        className='w-full h-10 rounded-full border-2 border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4 focus:outline-0'
                        name="email" 
                        value={changedDetails.email} 
                        onChange={handleChange} 
                    />
                </div>
                <div className='w-1/2 flex flex-col justify-start items-start px-2'>
                    <label className='text-sm text-orange-600 font-medium mb-1 ml-3'>Phone Number: </label>
                    <input
                        type="tel"
                        className='w-full h-10 rounded-full border-2 border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4 focus:outline-0' 
                        name="phone_number" 
                        value={changedDetails.phone_number} 
                        onChange={handleChange} 
                    />
                </div>
                <div className='w-1/2 flex flex-col justify-start items-start px-2'>
                    <label className='text-sm text-orange-600 font-medium mb-1 ml-3'>City: </label>
                    <select
                        value={changedDetails.city}
                        onChange={handleAreaChange}
                        className='w-full h-10 rounded-full border-2 focus:outline-none border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4'
                    >
                        <option value="">Select City</option>
                        {majorCities.filter(item => item.name === selectedCity)[0]?.area.map(city => (
                            <option key={city} value={city}>{city}</option>
                        ))}
                    </select>
                </div>
                <div className='w-1/2 flex flex-col justify-start items-start px-2'>
                    <label className='text-sm text-orange-600 font-medium mb-1 ml-3'>Area: </label>
                    <select
                        value={changedDetails.area}
                        onChange={handleCityChange}
                        className='w-full h-10 rounded-full border-2 focus:outline-none border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4'
                    >
                        <option value="">Select Area</option>
                        {majorCities.map(city => (
                                <option key={city.name} value={city.name}>{city.name}</option>
                        ))}
                    </select>
                </div>
                {changedDetails.user_type === "driver" &&
                <>
                    <div className='w-1/2 flex flex-col px-2'>
                        <label className='text-sm text-orange-600 font-medium mb-1'>License No: </label>
                        <input
                            type="text"
                            className='w-full h-10 rounded-full border-2 border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4' 
                            name="changedDetails.driver_additional_details.license_no" 
                            value={changedDetails.driver_additional_details?.license_no || ""} 
                            onChange={handleChange} 
                        />
                    </div>
                    <div className='w-1/2 flex flex-col px-2'>
                        <label className='text-sm text-orange-600 font-medium mb-1'>Aadhaar No: </label>
                        <input
                            type="text"
                            className='w-full h-10 rounded-full border-2 border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4' 
                            name="changedDetails.driver_additional_details.aadhaar_no" 
                            value={changedDetails.driver_additional_details?.aadhaar_no || ""} 
                            onChange={handleChange} 
                        />
                    </div>
                    <div className='w-1/2 flex flex-col px-2'>
                        <label className='text-sm text-orange-600 font-medium mb-1'>Vehicle: </label>
                        <input
                            type="text"
                            className='w-full h-10 rounded-full border-2 border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4' 
                            name="changedDetails.driver_additional_details.vehicle" 
                            value={changedDetails.driver_additional_details?.vehicle || ''} 
                            onChange={handleChange} 
                        />
                    </div>
                    <div className='w-1/2 flex flex-col px-2'>
                        <label className='text-sm text-orange-600 font-medium mb-1'>Vehicle Type: </label>
                        <input
                            type="text"
                            className='w-full h-10 rounded-full border-2 border-orange-600 border-opacity-50 disabled:bg-opacity-50 disabled:text-opacity-70 px-4' 
                            name="changedDetails.driver_additional_details.vehicle_type"
                            value={changedDetails.driver_additional_details?.vehicle_type || ''} 
                            onChange={handleChange} 
                        />
                    </div>
                </>}
            </div>
            <div className='w-60 aspect-square flex items-center text-9xl text-gray-500 border-2 border-orange-600 border-opacity-50 bg-gray-200 justify-center rounded-full overflow-hidden'>
                {changedDetails.profile_photo? 
                <img
                    src={changedDetails.profile_photo}
                    className='w-full h-full object-contain'
                    alt=''
                /> : <FaUser />}
            </div>
        </div>

        {(JSON.stringify(changedDetails) !== JSON.stringify(userData)) &&
        <div className='mt-5 flex items-center justify-center gap-4'>
            <button
                onClick={handleSubmit}
                className='bg-green-600 disabled:opacity-60 w-28 h-10 flex items-center justify-center text-white font-medium rounded-full mt-5'
            >
                {!loader? "Submit" : <ThreeDots color='#ffffff' />}
            </button>
            <button
                onClick={() => setChangedDetails(userData)}
                className='bg-red-600 disabled:opacity-60 w-28 h-10 flex items-center justify-center text-white font-medium rounded-full mt-5'
            >
                Reset
            </button>
        </div>}

        {/* franchise partner details */}
        {/* <div className='max-w-[80rem]'>
            <div className={`w-full pt-12`}>
            {editable &&
            <div className='mb-2 w-full flex items-center justify-end'>
                <p 
                    className={`${(totalPercentage(changedDetails.another_additional_details) === 15)? "bg-green-600" : "bg-red-600"} text-nowrap bg-opacity-80 w-56 font-medium text-white h-7 flex items-center justify-center text-sm rounded-full px-6`}
                >
                    Total Percentage:{" "}
                    <span className=''>
                    {totalPercentage(changedDetails.another_additional_details)}%
                    </span>
                </p>
            </div>}

            {changedDetails.another_additional_details.map((item, index) => (
            <div className='w-full flex py-2  items-center border-b-2 last:border-b-0 border-black border-opacity-15 justify-between'>
                <p className='text-lg text-[#525252]'>
                    Partner {index+1}:
                </p>
                <div className='w-fit flex items-center justify-center gap-3'>
                    <input
                        type='text'
                        name='changedDetails.another_additional_details.name'
                        disabled={(!editable && !loader)? true : false}
                        className='disabled:bg-transparent text-right rounded-full disabled:px-0 focus:outline-none w-60 py-1 px-3 bg-white bg-opacity-50 text-nowrap capitalize'
                        value={item.name}
                        onChange={handleChange}
                    />
                    <p className='px-2 text-nowrap bg-orange-600 bg-opacity-50 backdrop-blur-sm text-white rounded-full w-fit py-1 text-xs font-medium'>
                        Share:{" "}
                        <input 
                        type='number'
                        name='changedDetails.another_additional_details.percentage'
                        className='disabled:bg-transparent text-black rounded-full disabled:px-0 focus:outline-none w-16 disabled:w-10 py-1 text-center disabled:text-right bg-white bg-opacity-50 text-nowrap'
                        value={item.percentage}
                        onChange={handleChange}
                        step={0.1}
                        />%
                    </p>
                </div>
            </div>))}

            {editable &&
            <div className='w-full flex items-center justify-center pt-6 relative'>
            <button
                onClick={handleSubmit}
                className='h-8 px-8 bg-green-600 rounded-full text-white font-medium'
            >
                {loader? <ThreeDots color='#fff' height={"16px"} /> : "Save"}
            </button>
            </div>}
        </div>
        </div> */}
    </div>
  )
}

export default UserSearchWindow