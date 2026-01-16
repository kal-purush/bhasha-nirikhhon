import { useState } from "react";
import axios from "axios";
import { useNavigate } from "react-router-dom";
import { toast } from "react-toastify";
import { useDispatch } from "react-redux";
import { setUserData } from "../Redux/Slices/AuthSlice/AuthSlice";


const baseAuthUrl = `https://upskilling-egypt.com:3005/api/auth`;

const useCustomFetch = () => {

  const dispatch =useDispatch(); 
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);

  const customFetch = (pathUrl: string, data: object, navigateTo: string) => {
    setLoading(true);

    axios
      .post(`${baseAuthUrl}/${pathUrl}`, data)
      .then((res) => {

        dispatch(setUserData(res.data.data));
        toast.success(res.data.message);
        navigate(navigateTo);
        setLoading(false);
      })
      .catch((error) => {
        toast.error(error.response?.data?.message || "Invalid data");
        setLoading(false);
      })
  };

  return { customFetch, loading };

};

export default useCustomFetch;