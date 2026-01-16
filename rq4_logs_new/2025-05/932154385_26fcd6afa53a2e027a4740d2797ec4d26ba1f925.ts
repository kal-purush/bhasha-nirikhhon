export const EMAIL_ACTIVATE = (email: string, token: string) => {
  return {
    html: () => `
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Activación de Cuenta</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            background-color: #f5f5f5;
        }
        .activation-container {
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
            max-width: 500px;
            width: 100%;
            text-align: center;
        }
        h1 {
            color: #333;
            margin-bottom: 20px;
        }
        p {
            margin-bottom: 25px;
            color: #666;
        }
        .activate-btn {
            background-color: #4CAF50;
            color: white;
            border: none;
            padding: 12px 24px;
            text-align: center;
            text-decoration: none;
            display: inline-block;
            font-size: 16px;
            margin: 10px 2px;
            cursor: pointer;
            border-radius: 4px;
            transition: background-color 0.3s;
        }
        .activate-btn:hover {
            background-color: #45a049;
        }
    </style>
</head>
<body>
    <div class="activation-container">
        <h1>Activa tu cuenta</h1>
        <p>¡Gracias por registrarte! Haz clic en el botón de abajo para activar tu cuenta y comenzar a disfrutar de nuestros servicios.</p>
        
        <form action="http://localhost:3000/auth/activate" method="POST">
            <input type="hidden" required name="email" value="${email ? email : null}" />
            <input type="hidden" required name="token" value="${token ? token : null} " />
            <button type="submit" class="activate-btn">Activar Cuenta</button>
        </form>
        
        <p>Si no solicitaste este registro, por favor ignora este mensaje.</p>
    </div>
</body>
</html>
    `,
  };
};