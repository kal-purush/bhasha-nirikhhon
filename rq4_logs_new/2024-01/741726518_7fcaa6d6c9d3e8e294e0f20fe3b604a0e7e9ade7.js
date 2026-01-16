const QDPAIStart = formatMessage => ({
    name: 'QDPRobot AIStart',
    deviceId: 'QDPAIStart_k210MaixDock',
    manufactor: 'QDPRobot',
    learnMore: 'http://gx.qdprobot.com',
    typeList: ['arduino', 'microPython'],
    iconURL: 'asset/QDPAIStart.png',
    typeList: ['arduino', 'microPython'],
    description: formatMessage({
        id: 'QDPAIStart.description',
        default: 'QDPRobot AIStart AI host.',
        description: 'Description for the QDP robot device'
    }),
    pnpidList: [
        'USB\\VID_1A86&PID_7523'
    ],
    connectionIconURL: 'asset/QDPAIStart-illustration.svg',
    connectionSmallIconURL: 'asset/QDPAIStart-small.svg',
    programLanguage: ['block', 'c', 'cpp', 'microPython'],
    tags: ['kit'],
    helpLink: 'http://gx.qdprobot.com/'
});

const QDPAIStartArduino = formatMessage => {
    const device = QDPAIStart(formatMessage);
    device.defaultBaudRate = '115200';
    device.deviceId = 'QDPAIStart_arduinoK210MaixDock';
    device.programMode = ['upload'];
    device.deviceExtensions = ['arduinoQDPAIStart'],
    device.deviceExtensionsCompatible = 'arduinoK210MaixDock',
    device.hide = true;
    return device;
}

const QDPAIStartMicroPython = formatMessage => {
    const device = QDPAIStart(formatMessage);
    device.defaultBaudRate = '115200';
    device.deviceId = 'QDPAIStart_microPythonK210MaixDock';
    device.programMode = ['upload'];
    device.deviceExtensions = ['microPythonQDPAIStart'],
    device.deviceExtensionsCompatible = 'microPythonK210MaixDock',
    device.hide = true;
    return device;
}

module.exports = formatMessage => ([
    QDPAIStart(formatMessage),
    QDPAIStartArduino(formatMessage),
    QDPAIStartMicroPython(formatMessage)
]);