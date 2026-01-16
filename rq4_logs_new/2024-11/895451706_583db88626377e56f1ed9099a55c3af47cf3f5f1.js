function generateData() {
    const servers = [];
    const ipAddresses = [];
    const environmentClasses = ['Промышленный', 'Тестовый'];
    const systems = ['АС Контроль', 'АС Мониторинг', 'АС Логистика', 'АС Отчётность', 'АС Склад', 'АС Производство', 'АС Финансы', 'АС HR', 'АС CRM', 'АС ERP'];

    for (let i = 0; i < 1500; i++) {
        const serverName = Array.from({ length: getRandomInt(4, 6) }, () => 
            String.fromCharCode(97 + getRandomInt(0, 25))
        ).join('') + getRandomInt(1, 99999);
        servers.push(serverName);

        const ip = `${getRandomInt(1, 255)}.${getRandomInt(0, 255)}.${getRandomInt(0, 255)}.${getRandomInt(1, 255)}`;
        ipAddresses.push(ip);
    }

    const data = servers.map((server, index) => ({
        name: server,
        ip: ipAddresses[index],
        environment: environmentClasses[getRandomInt(0, 1)],
        system: systems[getRandomInt(0, systems.length - 1)],
    }));

    localStorage.setItem('serverData', JSON.stringify(data));
}

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

generateData();