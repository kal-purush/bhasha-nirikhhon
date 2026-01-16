import {ChangeEvent, FormEvent, useState} from "react";
import {Character, Race} from "../../types";
import {useNavigate} from "react-router-dom";
import {useMutation} from "@apollo/client";
import {CREATE_CHARACTER} from "../../queries/character";

export const useCreateCharacter = () => {
    const navigate = useNavigate();
    const [newCharacter, setNewCharacter] = useState<Character>({
        firstName: '',
        lastName: '',
        age: 18,
        race: Race.HUMAN,
        bio: '',
    });
    const [error, setError] = useState({
        firstName: '',
        lastName: '',
    });
    const [createCharacter, _] = useMutation(CREATE_CHARACTER)

    const onCreate = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        if (newCharacter.firstName.length > 0 && newCharacter.firstName.length <= 50) {
            if (newCharacter.lastName.length > 0 && newCharacter.lastName.length <= 50) {
                createNewCharacter()
                navigate('/list');
            } else {
                setError({...error, lastName: 'Please enter last name!'});
            }
        } else {
            setError({...error, lastName: 'Please enter last name!'});
        }
    }

    const createNewCharacter = async () => {
        await createCharacter({
            variables: {

            }
        }).then(_ => navigate('/list'))
            .catch(error => alert(error.message));
    }

    const handleInput = (event: ChangeEvent<
                                HTMLInputElement |
                                HTMLSelectElement |
                                HTMLTextAreaElement>) => {
        setNewCharacter({...newCharacter, [event.target.name]: event.target.value});
        if (event.target.name === 'firstName' || event.target.name === 'lastName'){
            setError({...error, [event.target.name]: ''});
        }
    }

    return {
        newCharacter, handleInput, onCreate, error
    }
}