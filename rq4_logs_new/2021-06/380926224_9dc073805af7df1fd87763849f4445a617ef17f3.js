import { css } from "lit-element";

export default css`
:host {
        display: block;
      }
      .container {
        text-align: center;
      }
      get-data {
        display: none;
      }
      .card {
        display: inline-block;
        height: 300px;
        width: 200px;
        background: rgba(172, 255, 95, 0.35);
        border-radius: 10px;
        margin: 1rem;
        position: relative;
        text-align: center;
        transition:all 0.3s cubic-bezier(0.25, .8, 0.25, 1)
      }
      .card img {
        width: 70%;
      }
`