
document.addEventListener('DOMContentLoaded', function() {
    const timelineItems = document.querySelectorAll('.timeline-item');
    
    function isInViewport(element) {
        const rect = element.getBoundingClientRect();
        return (
            rect.top >= 0 &&
            rect.left >= 0 &&
            rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
            rect.right <= (window.innerWidth || document.documentElement.clientWidth)
        );
    }
    
    function handleScroll() {
        timelineItems.forEach(item => {
            if (isInViewport(item)) {
                item.classList.add('animate');
            }
        });
    }
    
    handleScroll();
    
    window.addEventListener('scroll', handleScroll);
    
    const valueCards = document.querySelectorAll('.value-card');
    
    valueCards.forEach(card => {
        card.addEventListener('mouseenter', () => {
            valueCards.forEach(c => c.classList.remove('active'));
            card.classList.add('active');
        });
    });
    
    const statNumbers = document.querySelectorAll('.stat-number');
    
    function animateValue(obj, start, end, duration) {
        let startTimestamp = null;
        const step = (timestamp) => {
            if (!startTimestamp) startTimestamp = timestamp;
            const progress = Math.min((timestamp - startTimestamp) / duration, 1);
            let value = Math.floor(progress * (end - start) + start);
            
            if (obj.textContent.includes('+')) {
                value = value + '+';
            } else if (obj.textContent.includes('%')) {
                value = value + '%';
            }
            
            obj.textContent = value;
            if (progress < 1) {
                window.requestAnimationFrame(step);
            }
        };
        window.requestAnimationFrame(step);
    }
    
    function startCounters() {
        statNumbers.forEach(stat => {
            const value = parseInt(stat.textContent.replace(/[^\d]/g, ''));
            animateValue(stat, 0, value, 2000);
        });
    }
    
    const impactStats = document.querySelector('.impact-stats');
    if (impactStats) {
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    startCounters();
                    observer.unobserve(entry.target);
                }
            });
        });
        
        observer.observe(impactStats);
    }
});