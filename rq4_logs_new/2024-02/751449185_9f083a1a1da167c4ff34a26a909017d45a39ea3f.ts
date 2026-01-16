import axios from 'axios';
import { useEffect, useState } from "react";
import { useSelector } from "react-redux";
import { toast } from 'react-toastify';
import { baseUrl } from './ApiUtls';

const useFetchData = () => {
    const [students, setStudents] = useState([])
    const { headers } = useSelector((state: any) => state.userData)
    


    const getData = (path:string) => {
        axios.get(`${baseUrl}/${path}`, headers).then((response) => {

            setStudents(response.data)
        }).catch((error) => {
            toast.error(error.response.data.message || 'Invalid Data')

        })
    }
useEffect(()=>{getData('student')},[])
    return { students, getData }

}
export default useFetchData;