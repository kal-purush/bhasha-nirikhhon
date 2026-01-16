

import React, { useEffect, useState } from "react";
import { imageDb, auth } from '../../firebase'; // Assuming 'auth' is the Firebase authentication module
import { getDownloadURL, listAll, ref, uploadBytes } from "firebase/storage";
import { v4 } from "uuid";

function Home(props) {
    const [img, setImg] = useState('')
    const [imgUrl, setImgUrl] = useState([])
    const [user, setUser] = useState(null);

    useEffect(() => {
        // Check if user is authenticated
        const unsubscribe = auth.onAuthStateChanged((user) => {
            if (user) {
                setUser(user);
            } else {
                setUser(null);
            }
        });
        return unsubscribe;
    }, []);

    const handleClick = () => {
        if (img !== null && user) {
            const imgRef = ref(imageDb, `files/${user.uid}/${v4()}`); // Store images under user's uid
            uploadBytes(imgRef, img).then(value => {
                console.log(value);
                getDownloadURL(value.ref).then(url => {
                    setImgUrl(data => [...data, url]);
                });
            });
        }
    }

    useEffect(() => {
        if (user) {
            listAll(ref(imageDb, `files/${user.uid}`)).then(imgs => {
                console.log(imgs);
                imgs.items.forEach(val => {
                    getDownloadURL(val).then(url => {
                        setImgUrl(data => [...data, url]);
                    });
                });
            });
        }
    }, [user]);

    return (
        <div className="App">
            <div>
                <h2>{user ? `Welcome - ${user.displayName}` : "Login please"}</h2>
            </div>

            {user &&
                <div>
                    <input type="file" onChange={(e) => setImg(e.target.files[0])} />
                    <button onClick={handleClick}>Upload</button>
                </div>
            }
            <br />
            {
                imgUrl.map(dataVal => (
                    <div key={dataVal}>
                        <img src={dataVal} height="200px" width="200px" alt="User's Photo" />
                        <br />
                    </div>
                ))
            }
        </div>
    )
}
export default Home;