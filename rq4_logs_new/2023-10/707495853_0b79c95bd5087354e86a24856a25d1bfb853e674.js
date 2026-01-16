import React from "react";
import Btn from "../../../elements/Button";

function WelcomeDemo() {
  return (
    <div class="flex flex-col justify-center gap-y-8 p-20">
      <h2>
        Welcome to the <span>Demo</span>!
      </h2>
      <p class="text-center leading-10">
        In this quick interactive demo,
        <br />
        Please consider yourselft <span>working</span> at an{" "}
        <span>imaginary company</span>
        called <span>Easy & Instant.com</span>
        <br />
        You can see how <span>quickly and easily</span> you can order through
        your company portal using our service.
      </p>
      <div class="flex justify-center">
        <Btn label="Start Demo" to="/demostep1"></Btn>
      </div>
    </div>
  );
}

export default WelcomeDemo;