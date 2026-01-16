import Image from "next/image"
import { useMoralis } from "react-moralis"

function Avatar({username, logoutOnPress}) {

    const { user, logout } = useMoralis()

    console.log(user)

    return <Image 
    className="rounded-full bg-black cursor-pointer"
    src={`https://avatars.dicebear.com/api/pixel-art/${user.get("username")}.svg`}
    layout="fill"
    onClick={() => logoutOnPress && logout()}
    />
}

export default Avatar