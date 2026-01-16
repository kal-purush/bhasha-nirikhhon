import { useEffect, useState } from "react";
import { database } from "../../services/firebase";
import { useAuth } from "../useAuth";

type FAQ = {
    titulo: string,
    email: string,
    categoria: string,
    descricao: string,
    nome: string,
    status: string
}

export function useGetFaqByID(faqId: string | undefined){
    const { user } = useAuth();
    const [ faq, setFaq ] = useState<FAQ>()

    useEffect(()=>{
        
        const faqRef = database.ref(`faq/${user?.id}/${faqId}`)

        faqRef.once('value', faq=>{
            const faqValue = faq.val();

            setFaq({
                titulo: faqValue.title,
                email: faqValue.email,
                categoria: faqValue.category,
                descricao: faqValue.description,
                nome: faqValue.name,
                status: faqValue.status
            })
        })
    },[user, faqId])

    return{faq}
}