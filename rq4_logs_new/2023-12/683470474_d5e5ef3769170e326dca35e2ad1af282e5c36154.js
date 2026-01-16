import { useState } from "react";

function Image({ src, alt, style, bstyle }) {
  const [active, setActive] = useState(false);

  return (
    <>
      <img 
      src={src} 
      alt={alt} 
      style={{cursor: "pointer", ...style}} 
      onClick={() => setActive(true)} 
    />

      {active && (
        <div
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "flex-start",

            position: "fixed",
            top: "0",
            left: "0",
            width: "100%",
            height: "100%",
            zIndex: "100",

            overflow: "auto",

            backgroundColor: "rgba(0, 0, 0, 0.50)",
          }}
          onClick={() => setActive(false)}
        >
          <img 
          src={src} 
          alt={alt} 
          style={{cursor: "pointer", ...bstyle}} />
        </div>
      )}
    </>
  );
}

export default Image;