import { useEffect, useState } from "react";
import { database } from "../../services/firebase";
import { useAuth } from "../useAuth";
import moment from "moment";
import { clearScreenDown } from "readline";


type FirebaseRecursos = Record<string, {
    id: string,
    author:{
        authorId: string,
        authorName: string,
        authorAvatar: string
    },
    category: string,
    type: string,
    startDate: string,
    endDate: string,
    title: string,
    canceled: boolean,
    state: string,
    street: string,
    url: string,
}>

type Recurso = {
    id: string,
    author: {
        authorId: string,
        authorName: string,
        authorAvatar: string
    },
    categoria: string,
    tipo: string,
    dataInicio: string,
    dataFinal: string,
    titulo: string,
    cancelado: boolean,
    estado: string,
    cidade: string,
    url: string,
}

//eventType is the guy that will tell us if we should return all events or just mine
export function useGetAllRecursos(date: string, categoria: string, tipo: string, estado: string, cidade: string, cancelado: boolean){
    const [ recursosValues, setRecursosValues] = useState<Recurso[]>([]);
    const [ loadingRecursos, segLoadingRecursos ] = useState(true)

    const { user } = useAuth();

    async function processFilters(results: Recurso[]) {
        let takeToShare:Recurso[] = []
        await results.forEach(element => {
            if(moment(element.dataFinal).isAfter() && element.cancelado === false){
                takeToShare.push(element)
            }
        });

        await setRecursosValues(takeToShare)
        await segLoadingRecursos(false)
    }


    useEffect(() =>{
        const recursoRef = database.ref(`recursos`);

        recursoRef.on('value', recurso => {
            //console.log(evento.val())
            const databaseRecursos = recurso.val();

            const firebaseRecurso: FirebaseRecursos = databaseRecursos ?? {};
            
            const parsedRecursos = Object.entries(firebaseRecurso).map(([key, value])=>{
                return{
                    id: key,
                    author: value.author,
                    categoria: value.category,
                    tipo: value.type,
                    dataInicio: value.startDate,
                    dataFinal: value.endDate,
                    titulo: value.title,
                    cancelado: value.canceled,
                    estado: value.state,
                    cidade: value.street,
                    url: value.url,
                }
            }) 
            
            processFilters(parsedRecursos);
            
        })

    }, [user, date, categoria, tipo, estado, cidade, cancelado])


    return{recursosValues, setRecursosValues, loadingRecursos}
}