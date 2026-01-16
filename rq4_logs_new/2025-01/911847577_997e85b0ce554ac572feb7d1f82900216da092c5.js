// Neural Network Background
const canvas = document.getElementById('neural-network');
const ctx = canvas.getContext('2d');

// Set canvas size
function resizeCanvas() {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
}
resizeCanvas();
window.addEventListener('resize', resizeCanvas);

// Neural network nodes
class Node {
    constructor() {
        this.x = Math.random() * canvas.width;
        this.y = Math.random() * canvas.height;
        this.vx = (Math.random() - 0.5) * 0.5;
        this.vy = (Math.random() - 0.5) * 0.5;
    }

    update() {
        this.x += this.vx;
        this.y += this.vy;

        if (this.x < 0 || this.x > canvas.width) this.vx *= -1;
        if (this.y < 0 || this.y > canvas.height) this.vy *= -1;
    }
}

// Create nodes
const nodes = Array.from({ length: 50 }, () => new Node());

// Animation
function animate() {
    ctx.fillStyle = 'rgba(18, 0, 28, 0.15)';
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    nodes.forEach(node => {
        node.update();
    });

    // Draw connections
    nodes.forEach((nodeA, i) => {
        nodes.slice(i + 1).forEach(nodeB => {
            const distance = Math.hypot(nodeA.x - nodeB.x, nodeA.y - nodeB.y);
            if (distance < 150) {
                ctx.beginPath();
                ctx.moveTo(nodeA.x, nodeA.y);
                ctx.lineTo(nodeB.x, nodeB.y);
                ctx.strokeStyle = `rgba(255, 255, 255, ${1 - distance / 150})`;
                ctx.stroke();
            }
        });
    });

    requestAnimationFrame(animate);
}
animate();

// Theme Toggle
const themeToggle = document.querySelector('.theme-toggle');
const body = document.body;

themeToggle.addEventListener('click', () => {
    body.classList.toggle('light-theme');
    themeToggle.innerHTML = body.classList.contains('light-theme') 
        ? '<i class="fas fa-moon"></i>' 
        : '<i class="fas fa-sun"></i>';
});

// Scroll to Top
const scrollTop = document.querySelector('.scroll-top');

window.addEventListener('scroll', () => {
    if (window.pageYOffset > 300) {
        scrollTop.classList.add('visible');
    } else {
        scrollTop.classList.remove('visible');
    }
});

scrollTop.addEventListener('click', () => {
    window.scrollTo({
        top: 0,
        behavior: 'smooth'
    });
});