let age = 20;
let name = "sm";
function Greeting() {
  return <h1 style={{ color: "white" }}>안녕하세요 </h1>;
}

function RedButton(props) {
  return (
    <div>
      <button style={{ color: props.color, width: "50", height: "50" }} />
    </div>
  );
}

export { age, name, Greeting };

//하나만 export 한다면 export default