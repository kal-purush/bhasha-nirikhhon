import React, { useEffect } from 'react';
import {useRouter} from 'next/router'
const index = () => {
    const router = useRouter();
    useEffect(() => {
        router.push('/auth/signin')
    },[])
    return null;
}

export default index;