import { FileDrop } from "react-file-drop";
import { useState } from "react";

export default function Upload({children, onUploadFinish}) {
  const [isFileNearby, setIsFileNearby] = useState(false);
  const [isFileOver,setIsFileOver] = useState(false);
  const [isUploading,setIsUploading] = useState(false);

  function uploadImage(files, e) {
    e.preventDefault();
    setIsFileNearby(false);
    setIsUploading(true);
    const data = new FormData();
    data.append('post', files[0]);
    fetch('/api/upload', {
      method: 'POST',
      body: data,
    }).then(async response => {
      const json = await response.json();
      const src = json.src;
      onUploadFinish(src);
      setIsUploading(false);
    })
  }

  return (
    <FileDrop
      onDrop={uploadImage}
      onDragOver={() => setIsFileOver(true)}
      onDragLeave={() => setIsFileOver(false)}
      onFrameDragEnter={() => setIsFileNearby(true)}
      onFrameDragLeave={() => setIsFileNearby(false)}
      onFrameDrop={() => {
        setIsFileNearby(false);
        setIsFileOver(false);
      }}
    >
      <div className="relative">
        {(isFileOver || isFileNearby) && (
          <div className="bg-litterBlue absolute inset-0 rounded-lg border border-litterBorder flex justify-center items-center text-white">drop here!</div>
        )}
        {children({isUploading})}
      </div>
    </FileDrop>
  )
}