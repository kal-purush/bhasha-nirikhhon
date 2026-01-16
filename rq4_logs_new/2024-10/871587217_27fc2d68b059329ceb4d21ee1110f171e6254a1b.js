// Cursor Trail Effect
const cursorTrail = document.getElementById('cursor-trail');

// Create multiple dots for a smooth trail
const dots = [];
const maxDots = 15;

for (let i = 0; i < maxDots; i++) {
    const dot = document.createElement('div');
    dot.classList.add('cursor-dot');
    cursorTrail.appendChild(dot);
    dots.push(dot);
}

let currentDot = 0;

document.addEventListener('mousemove', (e) => {
    dots[currentDot].style.left = `${e.clientX}px`;
    dots[currentDot].style.top = `${e.clientY}px`;
    dots[currentDot].style.opacity = 1;
    dots[currentDot].style.transform = 'translate(-50%, -50%) scale(1)';
    
    setTimeout(() => {
        dots[currentDot].style.opacity = 0;
        dots[currentDot].style.transform = 'translate(-50%, -50%) scale(0)';
    }, 500); // Duration the dot stays visible
    
    currentDot = (currentDot + 1) % maxDots;
});

// Gyro Effect for Background
document.addEventListener('mousemove', (event) => {
    const { clientX, clientY } = event;
    const windowWidth = window.innerWidth;
    const windowHeight = window.innerHeight;

    // Calculate rotation angles
    const rotateY = ((clientX / windowWidth) - 0.5) * 20; // Rotate between -10deg to +10deg
    const rotateX = ((clientY / windowHeight) - 0.5) * -20; // Rotate between -10deg to +10deg

    const bg = document.querySelector('.bg');
    bg.style.transform = `translate(-25%, -25%) rotateY(${rotateY}deg) rotateX(${rotateX}deg)`;
});

// Reset rotation when mouse leaves the window
document.addEventListener('mouseleave', () => {
    const bg = document.querySelector('.bg');
    bg.style.transform = `translate(-25%, -25%) rotateY(0deg) rotateX(0deg)`;
});