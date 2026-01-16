import moment from "moment";
import { useEffect, useState } from "react";
import { database } from "../../services/firebase";
import { useAuth } from "../useAuth";

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
    canceled: string,
    confirmados: Record<string, {
        id: string,
        confirmedByUserAvatar: string,
        confirmedByUserID: string,
        confirmedByUserName: string
    }>
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
    cancelado: string,
    confirmados: Array<{
        id: string,
        confirmedByUserAvatar: string,
        confirmedByUserID: string,
        confirmedByUserName: string
    }> | undefined
}

export function useGetMyAgenda(){

    const { user } = useAuth();
    const [ eventsAgenda, setEventsAgenda ] = useState<Evento[]>([])
    const [ loadingAgenda, segLoadingAgenda ] = useState(true)


    async function handleFilterProcess(result: Evento[]) {
        let takeToShare:Evento[] = []
        if(result !== undefined){
            await result.forEach(async element => {
                // element.author.authorId === user?.id
                if(moment(element.dataFinal).isAfter()){
                    if(element.confirmados!==undefined){
                        await element.confirmados.forEach(confir =>{
                            if(confir.confirmedByUserID === user?.id){
                                takeToShare.push(element);
                                console.log(element)
                            }
                        })
                    }  
                }          
            });
        }
        await setEventsAgenda(takeToShare)
        await segLoadingAgenda(false)
    }

    useEffect(()=>{
        if(user != undefined){
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
                        confirmados: Object.entries(value.confirmados ?? {}).map(([key, value])=>{
                            return{
                                id: key,
                                confirmedByUserAvatar: value.confirmedByUserAvatar,
                                confirmedByUserID: value.confirmedByUserID,
                                confirmedByUserName: value.confirmedByUserName
                            }
                        })
                    }
                })
                handleFilterProcess(parsedEvents);
            })
        }
    },[user])

    return{eventsAgenda, loadingAgenda}
}