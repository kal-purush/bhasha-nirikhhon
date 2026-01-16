import moment from "moment"
import { useEffect, useState } from "react"
import { database } from "../../services/firebase"
import { useAuth } from "../useAuth"
import { useGetUserProfile } from "../user/useGetUserProfile"

type FirebaseEventos = Record<string, {
    id: string,
    author: {
        authorId: string,
        authorName: string,
        authorAvatar: string
    },
    category: string,
    startDate: string,
    endDate: string,
    title: string,
    state: string,
    city: string,
    district: string,
    street: string,
    url: string,
    canceled: boolean
}>

type Evento = {
    id: string,
    author: {
        authorId: string,
        authorName: string,
        authorAvatar: string
    },
    categoria: string,
    dataInicio: string,
    dataFinal: string,
    titulo: string,
    estado: string,
    cidade: string,
    bairro: string,
    rua: string,
    url: string,
    cancelado: boolean
}

export function useGetMatch(){

    const { user } = useAuth();
    const { loadingUser, userDef} = useGetUserProfile(user?.id);
    const [ eventsMatch, setEventsMatch ] = useState<Evento[]>([])
    const [ loadingMatch, segLoadingMatch ] = useState(true)

    async function handleFilterProcess(result: Evento[]) {
        let takeToShare:Evento[] = []
        if(result !== undefined){
            await result.forEach(async element => {
                if(moment(element.dataFinal).isAfter() && !element.cancelado && element.author.authorId !== user?.id){
                    if(element.categoria === "Ação social afetiva" && userDef?.userInterests.acAfetiva){
                        takeToShare.push(element);
                    }else if(element.categoria === "Ação social racional com relação a fins" && userDef?.userInterests.acRelacaoFins){
                        takeToShare.push(element);
                    }else if(element.categoria === "Ação social racional com relação a valores" && userDef?.userInterests.acRelacaoValores){
                        takeToShare.push(element);
                    }else if(element.categoria === "Ação social tradicional" && userDef?.userInterests.acTradi){
                        takeToShare.push(element);
                    }
                }          
            });
        }
        await setEventsMatch(takeToShare)
        await segLoadingMatch(false)
    }
    
    useEffect(()=>{
        if(user != undefined && !loadingUser){
            const eventRef = database.ref(`eventos`);
            
            
            eventRef.once('value', evento => {
                const databaseEventos = evento.val();
                const firebaseEvent: FirebaseEventos = databaseEventos ?? {};

                const parsedEvents = Object.entries(firebaseEvent).map(([key, value])=>{
                    return{
                        id: key,
                        author: value.author,
                        categoria: value.category,
                        dataInicio: value.startDate,
                        dataFinal: value.endDate,
                        titulo: value.title,
                        estado: value.state,
                        cidade: value.city,
                        bairro: value.district,
                        rua: value.street,
                        url: value.url,
                        cancelado: value.canceled,
                    }
                })
                handleFilterProcess(parsedEvents);
            })
        }
    },[user, userDef])

    return{loadingMatch, eventsMatch}
}