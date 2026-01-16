import axios from 'axios';
import { useEffect, useState } from "react"
import { useSelector } from "react-redux"


const useAllStudents = () => {
    const [students, setStudents] = useState([])
    const { headers } = useSelector((state: any) => state.userData)
    const [loading, setLoading] = useState(false);

    const getAllStudents = () => {
        axios.get('https://upskilling-egypt.com:3005/api/student', headers).then((response) => {
            console.log(response);
            setStudents(response.data)
        }).catch((error) => {
            console.log(error);

        }).finally(() => {
            setLoading(false)
        })
    }
    useEffect(() => {
        getAllStudents();
    }, [])
    return { students, loading, getAllStudents }

}
export default useAllStudents;