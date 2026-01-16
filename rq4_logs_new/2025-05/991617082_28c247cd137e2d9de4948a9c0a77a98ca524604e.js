// Configuração inicial
document.addEventListener('DOMContentLoaded', function() {
    // Inicializar animações de rolagem
    initScrollAnimations();
    
    // Verificar aula atual e próximas aulas
    updateClassStatus();
    
    // Atualizar a cada minuto
    setInterval(updateClassStatus, 60000);
});

// Dados das aulas (inseridos diretamente no código)
const classData = {
    'segunda': {
        '08-30': { professor: 'Fernanda Constantino', link: 'https://meet.google.com/abc-defg-hij' },
        '10-00': { professor: 'Hayckar', link: 'https://meet.google.com/jkl-mnop-qrs' },
        '14-00': { professor: 'Pedro Ian', link: 'https://meet.google.com/tuv-wxyz-123' },
        '15-00': { professor: 'Ricardo Souza', link: 'https://meet.google.com/456-789a-bcd' },
        '16-00': { professor: 'Juliana Martins', link: 'https://meet.google.com/efg-hijk-lmn' },
        '18-00': { professor: 'Fernando Lima', link: 'https://meet.google.com/opq-rstu-vwx' },
        '20-00': { professor: 'Patrícia Alves', link: 'https://meet.google.com/yza-123b-456' }
    },
    'terca': {
        '08-30': { professor: 'Roberto Mendes', link: 'https://meet.google.com/789-cdef-ghi' },
        '10-00': { professor: 'Camila Santos', link: 'https://meet.google.com/jkl-mnop-qrs' },
        '14-00': { professor: 'Lucas Ferreira', link: 'https://meet.google.com/tuv-wxyz-123' },
        '15-00': { professor: 'Beatriz Gomes', link: 'https://meet.google.com/456-789a-bcd' },
        '16-00': { professor: 'Marcelo Dias', link: 'https://meet.google.com/efg-hijk-lmn' },
        '18-00': { professor: 'Daniela Rocha', link: 'https://meet.google.com/opq-rstu-vwx' },
        '20-00': { professor: 'Gustavo Pereira', link: 'https://meet.google.com/yza-123b-456' }
    },
    'quarta': {
        '08-30': { professor: 'Fernanda Constantino', link: 'https://meet.google.com/abc-defg-hij' },
        '10-00': { professor: 'André Carvalho', link: 'https://meet.google.com/jkl-mnop-qrs' },
        '14-00': { professor: 'Renata Vieira', link: 'https://meet.google.com/tuv-wxyz-123' },
        '15-00': { professor: 'Paulo Ribeiro', link: 'https://meet.google.com/456-789a-bcd' },
        '16-00': { professor: 'Cristina Moraes', link: 'https://meet.google.com/efg-hijk-lmn' },
        '18-00': { professor: 'Rodrigo Almeida', link: 'https://meet.google.com/opq-rstu-vwx' },
        '20-00': { professor: 'Tatiana Nunes', link: 'https://meet.google.com/yza-123b-456' }
    },
    'quinta': {
        '08-30': { professor: 'Leonardo Campos', link: 'https://meet.google.com/abc-defg-hij' },
        '10-00': { professor: 'Isabela Freitas', link: 'https://meet.google.com/jkl-mnop-qrs' },
        '14-00': { professor: 'Marcos Andrade', link: 'https://meet.google.com/tuv-wxyz-123' },
        '15-00': { professor: 'Carla Duarte', link: 'https://meet.google.com/456-789a-bcd' },
        '16-00': { professor: 'Rafael Barros', link: 'https://meet.google.com/efg-hijk-lmn' },
        '18-00': { professor: 'Aline Cardoso', link: 'https://meet.google.com/opq-rstu-vwx' },
        '20-00': { professor: 'Bruno Teixeira', link: 'https://meet.google.com/yza-123b-456' }
    },
    'sexta': {
        '08-30': { professor: 'Vanessa Pinto', link: 'https://meet.google.com/abc-defg-hij' },
        '10-00': { professor: 'Eduardo Mello', link: 'https://meet.google.com/jkl-mnop-qrs' },
        '14-00': { professor: 'Natália Ramos', link: 'https://meet.google.com/tuv-wxyz-123' },
        '15-00': { professor: 'Thiago Castro', link: 'https://meet.google.com/456-789a-bcd' },
        '16-00': { professor: 'Luciana Borges', link: 'https://meet.google.com/efg-hijk-lmn' },
        '18-00': { professor: 'Gabriel Moreira', link: 'https://meet.google.com/opq-rstu-vwx' },
        '20-00': { professor: 'Amanda Vasconcelos', link: 'https://meet.google.com/yza-123b-456' }
    }
};

// Horários das aulas
const classHours = [
    { hour: 8, minute: 30 },
    { hour: 10, minute: 0 },
    { hour: 14, minute: 0 },
    { hour: 15, minute: 0 },
    { hour: 16, minute: 0 },
    { hour: 18, minute: 0 },
    { hour: 20, minute: 0 }
];

// Duração da aula em minutos
const classDuration = 25;

// Dias da semana
const weekDays = ['domingo', 'segunda', 'terca', 'quarta', 'quinta', 'sexta', 'sabado'];

// Função para verificar se é dia útil
function isWeekday(day) {
    return day >= 1 && day <= 5; // 1 = segunda, 5 = sexta
}

// Função para verificar se há aula no momento
function getCurrentClass() {
    const now = new Date();
    const currentDay = now.getDay(); // 0 = domingo, 6 = sábado
    
    // Verificar se é dia útil
    if (!isWeekday(currentDay)) {
        return null;
    }
    
    const currentHour = now.getHours();
    const currentMinute = now.getMinutes();
    
    // Verificar cada horário de aula
    for (const classTime of classHours) {
        // Calcular o tempo atual em minutos desde o início do dia
        const nowInMinutes = currentHour * 60 + currentMinute;
        const classStartInMinutes = classTime.hour * 60 + classTime.minute;
        const classEndInMinutes = classStartInMinutes + classDuration;
        
        // Verificar se o horário atual está dentro do período da aula
        if (nowInMinutes >= classStartInMinutes && nowInMinutes < classEndInMinutes) {
            // Formatar o horário da aula
            const formattedHour = classTime.hour.toString().padStart(2, '0');
            const formattedMinute = classTime.minute.toString().padStart(2, '0');
            
            return {
                day: weekDays[currentDay],
                time: `${formattedHour}:${formattedMinute}`,
                hour: classTime.hour,
                minute: classTime.minute,
                formattedTime: `${formattedHour}-${formattedMinute}`
            };
        }
    }
    
    return null;
}

// Função para obter as próximas aulas
function getNextClasses() {
    const now = new Date();
    const currentDay = now.getDay();
    const currentHour = now.getHours();
    const currentMinute = now.getMinutes();
    const currentTimeInMinutes = currentHour * 60 + currentMinute;
    
    const nextClasses = [];
    
    // Se for dia útil, verificar as próximas aulas do dia atual
    if (isWeekday(currentDay)) {
        for (const classTime of classHours) {
            const classTimeInMinutes = classTime.hour * 60 + classTime.minute;
            
            if (classTimeInMinutes > currentTimeInMinutes) {
                const formattedHour = classTime.hour.toString().padStart(2, '0');
                const formattedMinute = classTime.minute.toString().padStart(2, '0');
                const formattedTime = `${formattedHour}-${formattedMinute}`;
                
                // Obter informações do professor
                const day = weekDays[currentDay];
                const professor = classData[day][formattedTime]?.professor || 'Professor não definido';
                
                nextClasses.push({
                    day: day,
                    time: `${formattedHour}:${formattedMinute}`,
                    hour: classTime.hour,
                    minute: classTime.minute,
                    dayOffset: 0,
                    professor: professor
                });
            }
        }
    }
    
    // Adicionar aulas dos próximos dias úteis se necessário
    let dayOffset = 1;
    let nextDay = (currentDay + dayOffset) % 7;
    
    // Continuar adicionando até ter pelo menos 5 próximas aulas ou ter verificado todos os dias da semana
    while (nextClasses.length < 5 && dayOffset < 7) {
        if (isWeekday(nextDay)) {
            for (const classTime of classHours) {
                const formattedHour = classTime.hour.toString().padStart(2, '0');
                const formattedMinute = classTime.minute.toString().padStart(2, '0');
                const formattedTime = `${formattedHour}-${formattedMinute}`;
                
                // Obter informações do professor
                const day = weekDays[nextDay];
                const professor = classData[day][formattedTime]?.professor || 'Professor não definido';
                
                nextClasses.push({
                    day: day,
                    time: `${formattedHour}:${formattedMinute}`,
                    hour: classTime.hour,
                    minute: classTime.minute,
                    dayOffset: dayOffset,
                    professor: professor
                });
                
                if (nextClasses.length >= 5) break;
            }
        }
        
        dayOffset++;
        nextDay = (currentDay + dayOffset) % 7;
    }
    
    return nextClasses.slice(0, 5); // Retornar apenas as 5 próximas aulas
}

// Função para atualizar o status da aula atual e próximas aulas
function updateClassStatus() {
    const statusAula = document.getElementById('status-aula');
    const linkAula = document.getElementById('link-aula');
    const btnAulaAtual = document.getElementById('btn-aula-atual');
    const listaProximasAulas = document.getElementById('lista-proximas-aulas');
    
    const currentClass = getCurrentClass();
    
    if (currentClass) {
        // Há uma aula acontecendo agora
        const day = currentClass.day;
        const formattedTime = currentClass.formattedTime;
        
        // Obter informações do professor e link da aula
        const professor = classData[day][formattedTime]?.professor || 'Professor não definido';
        const link = classData[day][formattedTime]?.link || '#';
        
        statusAula.innerHTML = `
            <p class="aula-atual-info">Aula em andamento!</p>
            <p><strong>Horário:</strong> ${currentClass.time}</p>
            <p><strong>Professor:</strong> ${professor}</p>
        `;
        
        btnAulaAtual.href = link;
        linkAula.classList.remove('hidden');
        
        // Adicionar classe de animação
        statusAula.classList.add('fade-in');
        setTimeout(() => {
            linkAula.classList.add('fade-in');
        }, 300);
    } else {
        // Não há aula no momento
        const nextClasses = getNextClasses();
        
        if (nextClasses.length > 0) {
            statusAula.innerHTML = `<p>Não há aula no momento.</p>`;
            linkAula.classList.add('hidden');
            
            // Atualizar lista de próximas aulas
            listaProximasAulas.innerHTML = '';
            
            nextClasses.forEach((nextClass, index) => {
                const day = nextClass.day;
                const time = nextClass.time;
                const dayOffset = nextClass.dayOffset;
                const professor = nextClass.professor;
                
                // Formatar o dia
                let dayText;
                if (dayOffset === 0) {
                    dayText = 'Hoje';
                } else if (dayOffset === 1) {
                    dayText = 'Amanhã';
                } else {
                    // Capitalizar o primeiro caractere do dia da semana
                    dayText = day.charAt(0).toUpperCase() + day.slice(1);
                }
                
                const li = document.createElement('li');
                li.innerHTML = `<strong>${dayText} às ${time}</strong> - ${professor}`;
                li.style.animationDelay = `${index * 0.1}s`;
                li.classList.add('fade-in');
                listaProximasAulas.appendChild(li);
            });
        } else {
            statusAula.innerHTML = `<p>Não há aulas programadas para os próximos dias.</p>`;
            linkAula.classList.add('hidden');
            listaProximasAulas.innerHTML = '<li class="fade-in">Nenhuma aula programada.</li>';
        }
    }
}

// Função para inicializar animações de rolagem
function initScrollAnimations() {
    // Selecionar todos os elementos com classe reveal
    const reveals = document.querySelectorAll('.reveal');
    
    // Função para verificar se o elemento está visível na tela
    function checkVisibility() {
        for (let i = 0; i < reveals.length; i++) {
            const windowHeight = window.innerHeight;
            const elementTop = reveals[i].getBoundingClientRect().top;
            const elementVisible = 150;
            
            if (elementTop < windowHeight - elementVisible) {
                reveals[i].classList.add('active');
            } else {
                reveals[i].classList.remove('active');
            }
        }
    }
    
    // Adicionar classe reveal a elementos que devem animar ao rolar
    const cards = document.querySelectorAll('.card');
    cards.forEach((card, index) => {
        if (!card.classList.contains('fade-in') && 
            !card.classList.contains('slide-in-right') && 
            !card.classList.contains('slide-in-left')) {
            card.classList.add('reveal');
            
            if (index % 2 === 0) {
                card.classList.add('fade-left');
            } else {
                card.classList.add('fade-right');
            }
        }
    });
    
    // Verificar visibilidade ao carregar a página
    checkVisibility();
    
    // Adicionar evento de rolagem
    window.addEventListener('scroll', checkVisibility);
    
    // Animar partículas
    animateParticles();
}

// Função para animar partículas
function animateParticles() {
    const particles = document.querySelectorAll('.particle');
    
    particles.forEach(particle => {
        // Posição inicial aleatória
        const randomX = Math.random() * 100;
        const randomY = Math.random() * 100;
        
        particle.style.left = `${randomX}%`;
        particle.style.top = `${randomY}%`;
    });
}