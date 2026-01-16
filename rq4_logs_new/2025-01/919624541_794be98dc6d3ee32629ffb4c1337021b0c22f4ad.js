function createConfetti(container) {
    container.innerHTML = "";

    for (let i = 0; i < 30; i++) {
        const confetti = document.createElement('div');
        confetti.classList.add('confetti-piece');

        // Cores aleatórias para o confetti
        const colors = ['#ffd700', '#ff3e00', '#00ff00', '#00ffff', '#ff00ff'];
        confetti.style.backgroundColor = colors[Math.floor(Math.random() * colors.length)];

        // Cálculo da dispersão em 360 graus
        const angle = Math.random() * Math.PI * 2;
        const spread = 20 + Math.random() * 60;

        // Calculando posições finais usando seno e cosseno para movimento circular
        const endX = Math.cos(angle) * spread;
        const endY = Math.sin(angle) * spread;

        confetti.style.setProperty('--end-x', `${endX}px`);
        confetti.style.setProperty('--end-y', `${endY}px`);

        // Variando velocidades e delays para movimento mais natural
        confetti.style.animationDelay = `${Math.random() * 0.2}s`;
        confetti.style.animationDuration = `${0.8 + Math.random() * 0.4}s`;

        // Rotação aleatória para cada peça
        confetti.style.setProperty('--rotation', `${Math.random() * 720 - 360}deg`);

        container.appendChild(confetti);
    }
}

export { createConfetti }; 