import "./styles.css";
// import IPFS from 'ipfs';
import React, { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import axios from "axios";

export default function App() {
  // const node = await IPFS.create()
  // console.log(IPFS, 'ssss');

  // const stream = node.cat('QmaXvbMnwAB9Mv9L3pYJhGqJ7P2ZnZLZUyTp7pvcCTu9ZL')
  // let data = ''

  // for await (const chunk of stream) {
  //   // chunks of data are returned as a Buffer, convert it back to a string
  //   data += chunk.toString()
  // }

  // console.log(data)

  return (
    <div className="App">
      <h1>Hello IPFS</h1>
      <MyDropzone />
    </div>
  );
}

function cdnUrl(url) {
  return "https://cloudflare-ipfs.com/ipfs/" + url;
}

function MyDropzone() {
  const [images, setImages] = useState([]);
  const onDrop = useCallback(
    (acceptedFiles) => {
      // Do something with the files
      console.log(acceptedFiles);
      var data = new FormData();
      data.append("path", acceptedFiles[0]);

      var config = {
        method: "post",
        url: "https://ipfs.infura.io:5001/api/v0/add?pin=true",
        data: data
      };

      axios(config)
        .then(function (response) {
          console.log(JSON.stringify(response.data));
          const imagesClone = [...images];
          imagesClone.push(response.data.Hash);
          setImages(imagesClone);
          console.log("imagesClone", images);
        })
        .catch(function (error) {
          console.log(error);
        });
    },
    [images]
  );

  const { getRootProps, getInputProps, isDragActive } = useDropzone({ onDrop });

  return (
    <div {...getRootProps()} style={{ border: "5px dotted red", height: 150 }}>
      <input {...getInputProps()} />
      {isDragActive ? (
        <p>Drop the files here ...</p>
      ) : (
        <p>Drag 'n' drop some files here, or click to select files</p>
      )}
      {images.length > 0 &&
        images.map((item) => <img src={cdnUrl(item)} width="100" />)}
    </div>
  );
}