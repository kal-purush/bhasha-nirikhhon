import { useEffect, useState } from "react"
import { database } from "../../services/firebase";
import { useAuth } from "../useAuth";

type FAQ = {
    id: string,
    autorId: string,
    titulo: string,
    email: string,
    categoria: string,
    descricao: string,
    nome: string,
    status: string
}

type FAQFireBase = Record<string,{
    id: string,
    authorId: string,
    title: string,
    email: string,
    category: string,
    description: string,
    name: string,
    status: string
}>

export function useGetUserFaq(){
    const { user } = useAuth();
    const [ faqList, setFaqList ] = useState<FAQ[]>()
    const [ faqListLoading, setFaqListLoading ] = useState<boolean>(true)


    useEffect(()=>{
        if(user !== undefined){
            const faqRef = database.ref(`/faq/${user.id}/`)

            faqRef.once('value', faq=>{
                const faqsValue = faq.val();
                const firebaseFaqs: FAQFireBase = faqsValue ?? {};

                const parsedEvents = Object.entries(firebaseFaqs).map(([key, value])=>{
                    return{
                        id: key,
                        autorId: value.authorId,
                        titulo: value.title,
                        email: value.email,
                        categoria: value.category,
                        descricao: value.description,
                        nome: value.name,
                        status: value.status
                    }
                })

                setFaqList(parsedEvents)
                setFaqListLoading(false)
            })
        }
    },[user])

    return{faqListLoading, faqList}
}