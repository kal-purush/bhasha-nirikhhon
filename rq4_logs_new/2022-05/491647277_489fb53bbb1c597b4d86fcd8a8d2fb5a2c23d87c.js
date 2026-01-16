import axios from "axios";

export const JokeOfTheDayAPI = axios.create({
    baseURL: 'https://v2.jokeapi.dev/joke/Any'
});

export const JokesAPI = axios.create({
    baseURL: 'https://v2.jokeapi.dev/joke/Any?blacklistFlags=nsfw&type=single&amount=5'
});