// jobSelectionCongrats.js
export default function ({id, title, name}, registrationLink) {
    return `
      <html>
      <head>
        <title>Congratulations on Your Job Selection</title>
        <style>
          body {
            font-family: Arial, sans-serif;
            background-color: #f4f4f9;
            margin: 0;
            padding: 0;
          }
          .container {
            width: 80%;
            margin: 20px auto;
            padding: 20px;
            background-color: #ffffff;
            border: 1px solid #dddddd;
            border-radius: 10px;
          }
          h1 {
            color: #4CAF50;
          }
          p {
            font-size: 16px;
            color: #333;
          }
          .button {
            background-color: #4CAF50;
            color: white;
            padding: 10px 20px;
            text-align: center;
            text-decoration: none;
            display: inline-block;
            font-size: 16px;
            margin-top: 20px;
            border-radius: 5px;
          }
          .tips {
            margin-top: 30px;
          }
          .tips h3 {
            color: #333;
          }
          .tips ul {
            padding-left: 20px;
          }
          .tips ul li {
            margin-bottom: 10px;
          }
        </style>
      </head>
      <body>
        <div class="container">
          <h1>Congratulations, ${name}!</h1>
          <p>We are thrilled to inform you that you have been selected for the title of <strong>${title}</strong> at our company.</p>
          <p>To complete your registration, please follow the link below:</p>
          <a href="${registrationLink}" class="button">Complete Your Registration</a>
          <p>If you have any questions, feel free to reach out to us. We look forward to having you onboard!</p>
          <p>Best regards, <br> The HR Team</p>
        </div>
      </body>
      </html>
    `;
  };
  

//   <div class="tips">
//             <h3>Here are some tips to help you get started:</h3>
//             <ul>
//               ${tips.map(tip => `<li>${tip}</li>`).join('')}
//             </ul>
//           </div>