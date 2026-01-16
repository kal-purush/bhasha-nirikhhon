import {useContext, useEffect, useState} from "react";
import {GlobalContext} from "../../context";
import {Character} from "../../types";
import {useLazyQuery} from "@apollo/client";
import {GET_USER_CHARACTERS} from "../../queries/character";
import {useNavigate} from "react-router-dom";

export const useCharacterList = () => {
    const {user, setUser} = useContext(GlobalContext);
    const navigate = useNavigate();
    const [getCharacters, {data, loading, refetch}] = useLazyQuery(GET_USER_CHARACTERS);

    useEffect(() => {
        if (user) {
            if (user.characters.length === 0) {
                fetchCharacters(user.id).then(result => {
                    if (result.data.getUserCharacters) {
                        setUser({
                            ...user,
                            characters: result.data.getUserCharacters
                        })
                    }
                }).catch(error => alert(error));
            }
        }
    }, []);

    const fetchCharacters = async (userId: number) => {
        return await getCharacters({
            variables: {
                userId
            }
        });
    }

    return {
        user
    }
}