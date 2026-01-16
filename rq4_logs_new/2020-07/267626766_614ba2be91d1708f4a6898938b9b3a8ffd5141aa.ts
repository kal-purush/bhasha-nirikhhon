import mongoose from "mongoose";

const root = (req: any, res: any) => {
    const mongooseState = 
        mongoose.connection.readyState === 0 ? 'DISCONNECTED ✖' :
        mongoose.connection.readyState === 1 ? 'CONNECTED ✔' :
        mongoose.connection.readyState === 2 ? 'CONNECTING ❕' :
        mongoose.connection.readyState === 3 ? 'DISCONNECTING ❕' :
        `UNKNOWN (${mongoose.connection.readyState})`;

    const boxStyle = `
        padding: 26px; 
        background-color: #F1F0F7; 
        border: 1px solid #FFF; 
        box-shadow: 1px 1px 0px #D1DBE8, 2px 2px 12px #E1EBE8;
        border-radius: 8px;
        max-width: 240px;
        height: 400px;

        margin: 40px;
        font-family: 'Fira Code', Verdana, sans-serif;
      `

    const htmlContent = `
      <div style="${boxStyle}">
        <h3 style="color:#080">SYSTEM ONLINE..</h3>
        <strong>DB:</strong> ${mongooseState}
      </div>
    `;

    return res.send(htmlContent);
};

export default {
  root,
};