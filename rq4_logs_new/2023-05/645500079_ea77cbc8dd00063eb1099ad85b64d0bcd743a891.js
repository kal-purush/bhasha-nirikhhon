import styled from "styled-components"
import { HiX } from "react-icons/hi";
import React from "react";
import { Link, useNavigate } from "react-router-dom";
import { useEffect } from "react";
import axios from "axios";


export default function SearchContainer ({searchOn, setSearchOn, searchInput}){


    const [listSugestions, setListSugestions] = React.useState(undefined)

    function closeSearchContainer(){
        setSearchOn(false)
    }


    useEffect(() => {
        if(searchOn === false) return

        const config = {
            headers: { "Authorization": `Bearer ${localStorage.getItem("TOKEN")}`}
          } 

          const body = {
            userName: searchInput
          }
          console.log("INPUT VALUE::::: ", searchInput)

          axios.post(`${process.env.REACT_APP_API_URL}/users/search`, body, config)
          .then((res) => {
            console.log("RESPOSTA DA SUGESTAO DE PESQUISA")
            console.log(res.data)
            setListSugestions(res.data)
          })
          .catch((err) => {
            console.log("deu erro")
            console.log(err)
          })

    }, [searchOn])

    if(listSugestions === undefined || listSugestions === null) return <>Loading</>
    

    return (
        <FollowersContainer searchOn={searchOn}>
            <Followers searchOn={searchOn}>
                <Botao onClick={closeSearchContainer}>
                    <HiX style={{color: "white", width: "30px", height: "30px"}}/>
                </Botao>
                <FoundSugestions>Sugestões Encontradas</FoundSugestions>
                <Space></Space>
                {listSugestions.map((user) => {
                    return (
                        <User 
                            key={user.id}
                            id={user.id} 
                            userImg={user.userImg} 
                            name={user.name}
                            biography={user.biography}

                        ></User>
                        )
                })}
            </Followers>
        </FollowersContainer>
    )
}

function User({id, name, userImg, biography}){

    const [visitId, setUserId] = React.useState(id)

    return (
        <UserContainer>
            <Link to={`/visit/${visitId}`}>
                <div>
                    <img src={userImg}></img>
                    <UserName>{name}</UserName>
                </div>
            </Link>
        </UserContainer>
    )
}

const FoundSugestions = styled.div`
    width: 220px;
    height: 40px;
    font-size: 20px;
    font-weight: 700;
    display: flex;
    justify-content: center;
    align-items: center;
`

const UserName = styled.div`
    font-size: 20px;
    font-weight: 500;

    width: 80px;
    height: auto;
    text-decoration: none;
    color: white;
    border: none;
`

const Space = styled.div`
    height: 50px;
`

const UserContainer = styled.div`
    width: 90%;
    height: 30px;
    color: white;
    border-bottom: 1px solid grey;
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 5px;
    
    button{
        width: 80px;
        border-radius: 5px;
        background-color: grey;
        color: white;
    }

    img {
        margin-right: 13px;
        width: 30px;
        height: 30px;
        border-radius: 20px;
    }

    div{
        display: flex;
        justify-content: space-around;
        align-items: center;
        width: 120px;
        height: auto;
    }
`
const Botao = styled.div`
    width: 30px;
    height: 30px;
    position: absolute;
    top: 10px;
    right: 10px;
`

const Followers = styled.div`
    background: #0A0A16;
    color: white;
    width: 50%;
    height: 80%;
    box-shadow: 0 0 10px 1px rgba(0, 0, 0, .25);
    transition: all .5s;
    transform: all .5s;
    opacity: ${(props) => props.searchOn ? "1" : "0"};
    flex-direction: column;
    justify-content: flex-start;
    align-items: center;
    margin-top: 70px;
    border-radius: 9px;
    position: relative;

    overflow-y: scroll;
`

const FollowersContainer = styled.div`
    width: 100vw;
    height: 100vh;
    position: fixed;
    top: 0;
    left: 0;
    background-color: rgba(50, 50, 50, 0.9);
    z-index: 1;
    display: ${(props) => props.searchOn ? "flex" : "none"};
    justify-content: center;
    align-items: center;
    font-size: 22px;
`