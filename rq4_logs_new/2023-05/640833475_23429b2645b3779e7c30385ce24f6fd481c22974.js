import React from "react";

const ColorCode = ({
  isComputing,
  coloredCode,
  coloredCodeRef,
  dim,
  globalOptions,
}) => {
  return (
    <div className="writeCode">
      {isComputing ? (
        <div>Compiling ...</div>
      ) : (
        <div
          style={{
            width: `${dim.width}px`,
            height: `${dim.height}px`,
            overflow: "hidden",
            background: globalOptions.background,
            lineHeight: `${globalOptions.lineheight}px`,
            fontSize: `${globalOptions.fontsize}px`,
            color: globalOptions.baseColor,
            letterSpacing: `-${globalOptions.fontsize / 22}px`,
          }}
          ref={coloredCodeRef}
          className="coloredCode"
        >
          <div
            style={{
              padding: `${globalOptions.padding}px`,
              borderRight: "1px solid black",
            }}
          >
            {coloredCode.map((_, index) => (
              <span
                className="numbering"
                style={{
                  lineHeight: `${globalOptions.lineheight}px`,
                  fontSize: `${(globalOptions.fontsize * 2.5) / 3}px`,
                }}
                key={index}
              >
                {index + 1}
              </span>
            ))}
          </div>
          <div style={{ padding: `${globalOptions.padding}px` }}>
            {coloredCode.map((instance, index) => (
              <span
                key={index}
                style={{
                  color: instance.colored ? instance.color : "",
                  fontWeight: instance.bold ? "700" : "500",
                }}
              >
                {instance.text}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default ColorCode;