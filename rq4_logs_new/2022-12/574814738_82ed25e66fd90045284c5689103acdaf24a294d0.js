import { useEffect, useState } from "react"

const useAdmin = email => {
    const [isAdmin, setIsAdmin] = useState(false);
    const [isUser, setIsUser] = useState(false);
    const [isSeller, setisSeller] = useState(false);
    const [isloading, setIsloading] = useState(true);

    useEffect(()=>{
        if(email){
            fetch(`http://localhost:5000/users/admin/${email}`)
            .then(res=> res.json())
            .then(data=> {
                console.log(data);
                setIsAdmin(data.isAdmin);
                setIsloading(false);
            })
            .catch(error=> {
                console.log(error)
            })
        }

        if(email){
            fetch(`http://localhost:5000/users/user/${email}`)
            .then(res=> res.json())
            .then(data=> {
                console.log(data)
                setIsUser(data.isUser);
            })
            .catch(error=> {
                console.log(error);
            })
        }


        if(email){
            fetch(`http://localhost:5000/users/seller/${email}`)
            .then(res=> res.json())
            .then(data=> {
                setisSeller(data.isSeller)
            })
            .catch(error=> {
                console.log(error)
            })
        }


    },[email])


    return [isAdmin, isUser, isSeller, isloading];
}

export default useAdmin;