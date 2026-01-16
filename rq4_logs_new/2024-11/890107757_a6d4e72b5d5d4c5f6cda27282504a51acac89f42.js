let playerY, computerY, ballX, ballY, ballSpeedX, ballSpeedY;
let paddleHeight, paddleWidth, ballSize, borderThickness;
let backgroundImage; // Imagem de fundo
let playerPaddleImage, computerPaddleImage; // Imagem raquetes
let ballImage; // // Imagem da bola
let ballScale = 1; // Escala da bola
let squashDuration = 10; // Duração da animação
let squashTimer = 0; // Temporizador para controlar a animação
let ballAngle = 0; // Ângulo inicial da bola
let bounceSound; // Som bola raquete
let goalSound; // Som de Goll
let playerScore = 0;
let computerScore = 0;
let goalScored = false; // Flag para garantir que o placar seja atualizado uma vez por gol
const maxBallSpeed = 15; // Limite de aumento da velocidade em 15 vezes
const winningScore = 3; // Definir o número de pontos necessários para vencer
let selectedOption = 0; // Variável para rastrear a seleção atual (índice)
let gameStarted = false;
let difficulty = 'easy';
const difficultyOptions = ["Fácil", "Médio", "Difícil"];
let initialScreen = true; // Variável para controlar se a tela inicial está sendo exibida
let startButtonX, startButtonY, startButtonWidth, startButtonHeight;
let buttonX, buttonY = [], buttonWidth, buttonHeight; // //Desenha os botoes de dificuldade
let playerName = 'Player 1'; // Nome padrão
let nameEntryScreen = false; // Novo estado para a tela de nome
let nameInputActive = false; // Controla se a tela de nome está ativa
let difficultySelectionScreen = false; // Estado para a tela de seleção de dificuldade
let showRestartButton = false; // Controla quando exibir o botão de reinício
let ball, ballVisible, ballResetTime;
let collisionSoundPlayed = false; // Variável de controle para o som de colisãolet collisionSoundPlayed = false; // Variável de controle para o som de colisão


function setup() {
    createCanvas(600, 400);
    //createCanvas(1360, 765);

    // Inicializar a tela inicial
    startButtonWidth = 200;
    startButtonHeight = 50;
    startButtonX = width / 2 - startButtonWidth / 2;
    startButtonY = height / 2 + 50;
    buttonWidth = 200;
    buttonHeight = 50;
    buttonX = width / 2 - buttonWidth / 2;
    let initialY = height / 2 - 75;
    buttonY = [height / 2 - 60, height / 2, height / 2 + 60]; // Posição dos botões
    paddleWidth = 10;
    paddleHeight = 100;
    ballSize = 15;
    borderThickness = 5;
    playerY = height / 2 - paddleHeight / 2;
    computerY = height / 2 - paddleHeight / 2;

    // Carrega sons e imagens
    backgroundImage = loadImage('sprites/fundo2.png');
    playerPaddleImage = loadImage('sprites/barra01.png');
    computerPaddleImage = loadImage('sprites/barra02.png');
    ballImage = loadImage('sprites/bola.png');
    bounceSound = loadSound('sounds/bounce.wav');
    goalSound = loadSound('sounds/goal.wav');
    resetBall();

    //BOLA
    ball = {
        x: width / 2,
        y: height / 2,
        radius: 12,
        speedX: 5,
        speedY: 3,
    };
    ballVisible = true; // Bola inicialmente visível
    ballResetTime = null; // Tempo em milissegundos antes da bola reaparecer

    // Inicializar a posição dos botões
    for (let i = 0; i < difficultyOptions.length; i++) {
        buttonY[i] = initialY + i * 60;
    }
}

function draw() {

    textFont('Protest Strike');

    if (initialScreen) {
        drawInitialScreen(); // Mostrar a tela inicial
        return;
    }

    if (nameEntryScreen) {
        drawNameEntryScreen(); // Mostrar a tela de entrada de nome
        return;
    }

    if (difficultySelectionScreen) {
        drawDifficultySelectionScreen(); // Mostrar a tela de seleção de dificuldade
        return;
    }

    if (gameStarted) {

        // Aqui, inicie o áudio ou qualquer lógica do jogo
        getAudioContext().resume();

        // Desenhar a imagem de fundo
        image(backgroundImage, 0, 0, width, height);

        // Remover as bordas de qualquer forma desenhada
        noStroke();

        // Definir a cor das barras superior e inferior
        fill(color("#318cd5"));

        // Desenhar a barra superior e inferior
        rect(0, 0, width, borderThickness);
        rect(0, height - borderThickness, width, borderThickness);

        // Restringir a raquete do jogador dentro dos limites das barras
        playerY = constrain(mouseY - paddleHeight / 2, borderThickness, height - paddleHeight - borderThickness);

        // Desenhar a raquete do jogador e do computador
        image(playerPaddleImage, 20, playerY, paddleWidth, paddleHeight);
        image(computerPaddleImage, width - 30, computerY, paddleWidth, paddleHeight);

        // Desenhar a bola com rotação
        push(); // Salvar o estado atual de transformação
        translate(ballX, ballY); // Mover o ponto de origem para a posição da bola
        rotate(ballAngle); // Aplicar a rotação
        scale(ballScale); // Aplicar a escala na bola
        image(ballImage, -ballSize / 2, -ballSize / 2, ballSize, ballSize); // Desenhar a imagem da bola
        pop(); // Restaurar o estado de transformação

        // Movimentar a bola
        ballX += ballSpeedX;
        ballY += ballSpeedY;

        // Calcular a velocidade total da bola (usando a magnitude da velocidade)
        let ballSpeed = sqrt(ballSpeedX * ballSpeedX + ballSpeedY * ballSpeedY);

        // Atualizar o ângulo da bola com base na velocidade
        ballAngle += ballSpeed * 0.05; // Ajustar o fator de rotação conforme necessário

        // Verificar colisão com as bordas
        handleWallCollision();

        // Chamar a função para mover a raquete do computador conforme nivel de fificuldade escolhido
        if (difficulty === 'easy') {
            moveComputerPaddleEasy();
        } else if (difficulty === 'medium') {
            moveComputerPaddleMedium();
        } else if (difficulty === 'hard') {
            moveComputerPaddleHard();
        }

        // Recalcular ângulos para evitar linha reta
        adjustBallAngle();

        // Verificar colisão com as raquetes
        if (ballX - ballSize / 2 <= 30 && ballY >= playerY && ballY <= playerY + paddleHeight) {
            handlePaddleCollision(playerY);
        } else if (ballX + ballSize / 2 >= width - 30 && ballY >= computerY && ballY <= computerY + paddleHeight) {
            handlePaddleCollision(computerY, true);
        }

        // Verificar gol (bola fora dos limites)
        if (!goalScored && ballX - ballSize / 2 <= 0) {
            computerScore++; // Computador marca um ponto
            goalSound.play(); // Tocar o som de gol
            goalScored = true; // Marcar que o gol foi detectado
            ballVisible = false; // Bola desativada
            collisionSoundPlayed = false;
            setTimeout(resetBallAfterGoal, ballResetTime); // Reiniciar a bola
        } else if (!goalScored && ballX + ballSize / 2 >= width) {
            playerScore++; // Jogador marca um ponto
            goalSound.play(); // Tocar o som de gol
            goalScored = true; // Marcar que o gol foi detectado
            ballVisible = false; // Bola desativada
            collisionSoundPlayed = false;
            setTimeout(resetBallAfterGoal, ballResetTime); // Reiniciar a bola
        }

        // Controlar o jogador com o mouse
        playerY = constrain(mouseY - paddleHeight / 2, 0, height - paddleHeight);

        // Lógica para restaurar a escala da bola
        if (squashTimer > 0) {
            squashTimer--;
        } else {
            ballScale = lerp(ballScale, 1, 0.1); // Gradualmente volta à escala normal (1x)
        }

        // Exibir o placar
        textAlign(CENTER);
        textSize(22);
        fill(255); // Cor do texto (branco)
        text(playerName, width / 4, 50); // Nome do Jogador
        text(playerScore, width / 4, 90); // Placar do jogador
        text("Player 2", 3 * width / 4, 50); // Nome do Jogador
        text(computerScore, 3 * width / 4, 90); // Placar do computador

        // Verificar se alguém ganhou
        if (playerScore >= winningScore || computerScore >= winningScore) {
            goalSound.play(); // Tocar o som de gol
            textSize(50);
            text("Ganhador", width / 2, height / 2);
            text(playerScore >= winningScore ? playerName : "Player 2!", width / 2, height / 2 + 60);

            showRestartButton = true; // Exibir o botão de reinício

            // Se o botão de reinício for visível, desenhá-lo
            if (showRestartButton) {
                drawRestartButton();
                noLoop();
            }
            return;
        }
    }
}

//Aumentar a velocidade da bola
function increaseSpeed() {
    // Aumenta a velocidade horizontal, limitado pelo valor máximo
    if (abs(ballSpeedX) < maxBallSpeed) {
        ballSpeedX *= 1.1; // Aumenta em 10%
        // Garante que não ultrapasse o máximo permitido
        if (abs(ballSpeedX) > maxBallSpeed) {
            ballSpeedX = ballSpeedX > 0 ? maxBallSpeed : -maxBallSpeed;
        }
    }
    // Aumenta a velocidade vertical, limitado pelo valor máximo
    if (abs(ballSpeedY) < maxBallSpeed) {
        ballSpeedY *= 1.1; // Aumenta em 10%
        // Garante que não ultrapasse o máximo permitido
        if (abs(ballSpeedY) > maxBallSpeed) {
            ballSpeedY = ballSpeedY > 0 ? maxBallSpeed : -maxBallSpeed;
        }
    }

    console.log(ballSpeedX)
}

//Colisão da bola com bordas 
function handleWallCollision() {
    // Colisão com a borda superior
    if (ballY - ballSize / 2 <= borderThickness) {
        ballY = borderThickness + ballSize / 2; // Reposicionar a bola
        ballSpeedY *= -1; // Inverter a direção Y da bola
        adjustBallAngle(); // Ajustar o ângulo para evitar trajetórias retas
    }

    // Colisão com a borda inferior
    if (ballY + ballSize / 2 >= height - borderThickness) {
        ballY = height - borderThickness - ballSize / 2; // Reposicionar a bola
        ballSpeedY *= -1; // Inverter a direção Y da bola
        adjustBallAngle(); // Ajustar o ângulo para evitar trajetórias retas
    }
}

//Ajusta angulo da bola
function adjustBallAngle() {
    // Evitar que a bola siga em linha reta vertical ou horizontal
    if (abs(ballSpeedY) < 1.5) {  // Valor ajustado para aumentar o ângulo
        ballSpeedY = random(2, 4) * (ballSpeedY < 0 ? -1 : 1); // Ajustar para garantir um ângulo mínimo
    }

    // Evitar que a bola siga em linha reta horizontal
    if (abs(ballSpeedX) < 2.5) {  // Valor ajustado para aumentar o ângulo
        ballSpeedX = random(4, 6) * (ballSpeedX < 0 ? -1 : 1); // Ajustar para garantir um ângulo mínimo
    }
}

// Direção da bola com base no toque das raquetes
function handlePaddleCollision(paddleY, isComputer = false) {
    ballSpeedX *= -1;

    // Reposicionar a bola fora da raquete
    if (isComputer) {
        ballX = width - 30 - ballSize / 2; // Reposicionar fora da raquete do computador
    } else {
        ballX = 30 + ballSize / 2; // Reposicionar fora da raquete do jogador
    }

    // Tocar o som de colisão apenas se o som não foi tocado nesta colisão
    if (!collisionSoundPlayed) {
        collisionSoundPlayed = true; // Impede que o som seja tocado novamente até que a bola se afaste
    }
    bounceSound.play(); // Toca o som de colisão com a raquete

    if (ballX > width / 2) {
        collisionSoundPlayed = false;
    }

    // Definir escala para o "esmagamento"
    ballScale = 0.6; // Achata a bola
    squashTimer = squashDuration; // Reiniciar o temporizador da animação

    // Calcula o ponto de impacto e ajusta a velocidade da bola
    let impactPoint;
    if (isComputer) {
        let offset = random(-paddleHeight / 4, paddleHeight / 4);
        impactPoint = (ballY + offset - paddleY) - paddleHeight / 2;
    } else {
        impactPoint = (ballY - paddleY) - paddleHeight / 2;
    }

    // Intensificar o ângulo
    let normalizedImpact = impactPoint / (paddleHeight / 2);
    ballSpeedY = ballSpeedY + (normalizedImpact * 4);// Aumente esse valor para intensificar o ângulo

    // Evitar que a bola se mova muito lentamente na vertical (linha reta)
    if (abs(ballSpeedY) < 1) {
        ballSpeedY = random(2, 4) * (ballSpeedY < 0 ? -1 : 1); // Garante um ângulo mínimo
    }

    // Evitar que a bola se mova muito lentamente na horizontal
    if (abs(ballSpeedX) < 2) {
        ballSpeedX = random(3, 5) * (ballSpeedX < 0 ? -1 : 1); // Garante que a velocidade horizontal não fique muito baixa
    }

    // Evitar movimento horizontal ou vertical em linha reta (quase zero)
    if (abs(ballSpeedY) < 1 || abs(ballSpeedX) < 1) {
        ballSpeedY = random(2, 4) * (ballSpeedY < 0 ? -1 : 1); // Força um ângulo se for muito reto
        ballSpeedX = random(3, 5) * (ballSpeedX < 0 ? -1 : 1); // Evitar que o valor de X seja muito pequeno
    }

    // Evitar que a bola "cole" na borda superior ou inferior
    if (ballY - ballSize / 2 <= borderThickness) {
        ballY = borderThickness + ballSize / 2;
        ballSpeedY = abs(ballSpeedY); // Força a bola a descer
    } else if (ballY + ballSize / 2 >= height - borderThickness) {
        ballY = height - borderThickness - ballSize / 2;
        ballSpeedY = -abs(ballSpeedY); // Força a bola a subir
    }

    increaseSpeed(); // Aumentar a velocidade da bola
}


// Funções para mover a raquete do computador: Dificil, médio e fácil
function moveComputerPaddleHard() {
    let randomOffset = random(-20, 20);// desvio na posição de destino os movimentos da raquete do computador
    let targetY = ballY - paddleHeight / 2 + randomOffset;
    let computerSpeed = 6; // Velocidade alta para o nível difícil
    let errorMargin = 10; // Menor margem de erro para movimentos mais precisos

    if (abs(computerY - targetY) > errorMargin) {
        if (computerY < targetY) {
            computerY += computerSpeed;
        } else if (computerY > targetY) {
            computerY -= computerSpeed;
        }
    }
    computerY = constrain(computerY, borderThickness, height - paddleHeight - borderThickness);
}
function moveComputerPaddleMedium() {
    let randomOffset = random(-20, 20);// desvio na posição de destino os movimentos da raquete do computador
    let targetY = ballY - paddleHeight / 2 + randomOffset;
    let computerSpeed = 4; // Velocidade média
    let errorMargin = 20; // Margem de erro média

    if (abs(computerY - targetY) > errorMargin) {
        if (computerY < targetY) {
            computerY += computerSpeed;
        } else if (computerY > targetY) {
            computerY -= computerSpeed;
        }
    }
    computerY = constrain(computerY, borderThickness, height - paddleHeight - borderThickness);
}
function moveComputerPaddleEasy() {
    let randomOffset = random(-20, 20);// desvio na posição de destino os movimentos da raquete do computador
    let targetY = ballY - paddleHeight / 2 + randomOffset;
    let computerSpeed = 2; // Velocidade baixa para o nível fácil
    let errorMargin = 40; // Margem de erro maior

    if (abs(computerY - targetY) > errorMargin) {
        if (computerY < targetY) {
            computerY += computerSpeed;
        } else if (computerY > targetY) {
            computerY -= computerSpeed;
        }
    }
    computerY = constrain(computerY, borderThickness, height - paddleHeight - borderThickness);
}

// Botao iniciar nova partida
function drawRestartButton() {
    // Definir a posição e o tamanho do botão
    let btnX = width / 2 - 100;
    let btnY = height / 2 + 100;
    let btnWidth = 200;
    let btnHeight = 50;

    // Desenhar o botão
    fill("#FFD700");
    rect(btnX, btnY, btnWidth, btnHeight, 30);

    fill(0);
    textSize(32);
    textAlign(CENTER, CENTER);
    text("Reiniciar", btnX + btnWidth / 2, btnY + btnHeight / 1.8);
}

// Reinicia partida
function restartGame() {
    // Resetar as variáveis do jogo para iniciar novamente
    playerScore = 0;
    computerScore = 0;
    ballX = width / 2;
    ballY = height / 2;
    ballSpeedX = 5;
    ballSpeedY = 5;
    showRestartButton = false; // Esconder o botão de reinício
    loop(); // Reiniciar o loop do jogo
}

// Reiniciar placar
function resetBallAfterGoal() {
    resetBall();
    goalScored = false; // Reiniciar a flag para permitir a contagem de gols novamente
    collisionSoundPlayed = false; // Resetar o som de colisão após o gol
}

// Reseta posição da bola
function resetBall() {
    ballX = width / 2;
    ballY = random(height / 8, 6 * height / 8); // Posição aleatória próxima do centro verticalmente
    ballSpeedX = random([-5, 5]); // Iniciar em uma direção aleatória
    ballSpeedY = random([-3, 3]); // Iniciar em uma direção aleatória
    ballVisible = true; // Mostrar a bola novamente
}

// Função para desenhar a tela inicial
function drawInitialScreen() {
    background(0);

    // Desenhar a imagem de fundo da tela inicial
    image(backgroundImage, 0, 0, width, height);

    // Definir o estilo do texto
    textAlign(CENTER);
    textSize(64);


    // Mostrar o texto do título
    text("Pong Game", width / 2, height / 2 - 100);

    // Verificar se o mouse está sobre o botão de start
    if (mouseX >= startButtonX && mouseX <= startButtonX + startButtonWidth &&
        mouseY >= startButtonY && mouseY <= startButtonY + startButtonHeight) {
        fill("#FFD700"); // Mudar a cor quando o mouse está sobre o botão
    } else {
        fill(255); // Cor normal do botão
    }

    // Verifica se o botão está selecionado (com teclado ou mouse)
    if (selectedOption === 0) {
        fill("#FFD700");  // Cor de destaque
    } else {
        fill(255);  // Cor normal
    }

    // Desenhar o botão de start
    rect(startButtonX, startButtonY, startButtonWidth, startButtonHeight, 30);
    fill(0);
    textSize(32);
    text("Iniciar", width / 2, startButtonY + startButtonHeight / 1.4);

    // Desenha instruções
    fill(255);
    textSize(20);
    text("Precione ENTER ou CLICK para iniciar", width / 2, height - 50);  // Exibe uma dica de controle na tela
}

function drawDifficultySelectionScreen() {
    background(0);

    // Desenhar a imagem de fundo da tela inicial
    image(backgroundImage, 0, 0, width, height);

    textAlign(CENTER);
    textSize(32);
    fill(255);

    text("Selecione modo do jogo", width / 2, height - 300);

    let mouseOverOption = -1; // Variável para rastrear se o mouse está sobre algum botão

    for (let i = 0; i < difficultyOptions.length; i++) {
        // Verifica se o mouse está sobre o botão
        if (mouseX >= buttonX && mouseX <= buttonX + buttonWidth &&
            mouseY >= buttonY[i] && mouseY <= buttonY[i] + buttonHeight) {
            mouseOverOption = i; // Atualiza para o índice do botão onde o mouse está
            selectedOption = i;  // Atualiza também a seleção do teclado com o índice do mouse
            fill("#FFD700"); // Cor do botão destacado pelo mouse
        } else if (i === selectedOption && mouseOverOption === -1) {
            // Se o mouse não estiver sobre nenhum botão, use a seleção do teclado
            fill("#FFD700"); // Cor do botão destacado pelo teclado
        } else {
            fill(255); // Cor padrão do botão
        }

        // Desenhar o botão
        rect(buttonX, buttonY[i], buttonWidth, buttonHeight, 40);

        // Desenhar o texto do botão
        fill(0);
        text(difficultyOptions[i], width / 2, buttonY[i] + buttonHeight / 1.4);
    }
}

function applyDifficultySelection() {
    switch (selectedOption) {
        case 0:
            difficulty = 'easy';
            break;
        case 1:
            difficulty = 'medium';
            break;
        case 2:
            difficulty = 'hard';
            break;
    }
    // Iniciar o jogo com a dificuldade selecionada
    //console.log("Dificuldade selecionada:", difficulty);
}


function drawNameEntryScreen() {
    background(0);
    image(backgroundImage, 0, 0, width, height);

    textAlign(CENTER);
    textSize(32);
    fill(255);
    text("Digite seu nome:", width / 2, height / 2 - 100);

    // Exibir o campo de entrada do nome
    fill(0);
    rect(width / 2 - 100, height / 2 - 50, 200, 50, 10);
    fill(255);
    text(playerName, width / 2, height / 2 - 15);

    // Botão para iniciar o jogo
    if (mouseX >= startButtonX && mouseX <= startButtonX + startButtonWidth &&
        mouseY >= startButtonY && mouseY <= startButtonY + startButtonHeight) {
        fill("#FFD700");
    } else {
        fill(255);
    }
    rect(startButtonX, startButtonY, startButtonWidth, startButtonHeight, 30);
    fill(0);
    text("Iniciar Jogo", width / 2, startButtonY + startButtonHeight / 1.4);

    // Desenhar instruções
    textSize(20);
    fill(255);
    text("Pressione ENTER ou clique em Iniciar Jogo", width / 2, height - 50);
}

// Função para lidar com eventos de teclas
function keyPressed() {
    if (initialScreen) {
        // Verifica se a tecla Enter foi pressionada na tela inicial
        if (keyCode === ENTER) {
            initialScreen = false;
            difficultySelectionScreen = true; // Vai para a tela de seleção de dificuldade
            return;
        }
    } else if (difficultySelectionScreen) {
        // Lógica para selecionar a dificuldade com teclas
        if (keyCode === UP_ARROW || keyCode === 87) { // Verifica se a tecla para cima ou 'W' foi pressionada
            selectedOption = (selectedOption - 1 + difficultyOptions.length) % difficultyOptions.length; // Navega para cima
        } else if (keyCode === DOWN_ARROW || keyCode === 83) { // Verifica se a tecla para baixo ou 'S' foi pressionada
            selectedOption = (selectedOption + 1) % difficultyOptions.length; // Navega para baixo
        } else if (keyCode === TAB) { // Tecla TAB para navegação
            selectedOption = (selectedOption + 1) % difficultyOptions.length; // Cicla para a próxima opção
        } else if (keyCode === ENTER) { // Verifica se a tecla Enter foi pressionada
            applyDifficultySelection(); // Aplica a seleção de dificuldade
            difficultySelectionScreen = false; // Sai da tela de seleção de dificuldade
            nameEntryScreen = true; // Vai para a tela de entrada de nome
        }
    } else if (nameEntryScreen) {
        // Aqui você pode adicionar a lógica para lidar com a tela de entrada de nome
        if (keyCode >= 65 && keyCode <= 90 || keyCode >= 48 && keyCode <= 57) {
            if (playerName.length < 10) {
                // Verifica se a tecla pressionada é uma letra ou número
                if ((keyCode >= 65 && keyCode <= 90) || (keyCode >= 48 && keyCode <= 57)) {
                    playerName += key; // Adiciona a letra ou número ao nome
                }
            }
        } else if (keyCode === BACKSPACE) {
            // Remove o último caractere se a tecla Backspace for pressionada
            playerName = playerName.slice(0, -1);
        } else if (keyCode === ENTER) {
            // Finaliza a entrada do nome e inicia o jogo
            gameStarted = true;
            nameEntryScreen = false;
        }
    } else if (showRestartButton) {
        if (keyCode === ENTER) {
            restartGame(); // Reiniciar o jogo se o botão for clicado
        }
    }
}

// Função para detectar cliques do mouse
function mousePressed() {
    if (initialScreen && mouseX >= startButtonX && mouseX <= startButtonX + startButtonWidth &&
        mouseY >= startButtonY && mouseY <= startButtonY + startButtonHeight) {
        initialScreen = false;
        difficultySelectionScreen = true; // Ir para a tela de seleção de dificuldade
    } else if (difficultySelectionScreen && mouseX >= buttonX && mouseX <= buttonX + buttonWidth) {
        for (let i = 0; i < difficultyOptions.length; i++) {
            if (mouseY >= buttonY[i] && mouseY <= buttonY[i] + buttonHeight) {
                selectedOption = i;
                applyDifficultySelection(); // Aplicar a dificuldade selecionada
                difficultySelectionScreen = false;
                nameEntryScreen = true; // Ir para a tela de entrada de nome
            }
        }
    } else if (nameEntryScreen && mouseX >= startButtonX && mouseX <= startButtonX + startButtonWidth &&
        mouseY >= startButtonY && mouseY <= startButtonY + startButtonHeight) {
        nameEntryScreen = false;
        gameStarted = true; // Iniciar o jogo
    } else if (showRestartButton) {
        let btnX = width / 2 - 100;
        let btnY = height / 2 + 100;
        let btnWidth = 200;
        let btnHeight = 50;

        if (mouseX > btnX && mouseX < btnX + btnWidth &&
            mouseY > btnY && mouseY < btnY + btnHeight) {
            restartGame(); // Reiniciar o jogo se o botão for clicado
        }
    }
}
