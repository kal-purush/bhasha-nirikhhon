


export const getPlayerList = async (userToken: string, userId:string) => {
    const res = await fetch(`${process.env.REACT_APP_BACKEND}/characters/${userId}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'x-auth-token': userToken,
            },
        }
    );
    const characterList = await res.json()

}