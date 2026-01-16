export const reportStyles = `
  <style>
    body {
      margin: 0;
      font-family: "sans-serif";
    }
    
    h1, h2, p, ol {
      margin: 0;
      padding: 0;
    }
    
    h1 {
      text-decoration: underline;
      margin: 0;
      padding: 5px 10px;
    }
    
    h2 {
       padding-left: 30px;
       padding-top: 10px;
       padding-bottom: 5px;
    }
    
    ol {
      padding-left: 30px;
      width: 80%;
    }
    
    li {
      display: flex;
      justify-content: space-between;
    }
    
    table {
      margin: 20px 10px;
      width: calc(100vw - 20px);
      border-collapse: collapse;
    }
    th {
    text-align: left;
    }
    th, td {
    border: 1px solid black;
    }
  </style>
  `;