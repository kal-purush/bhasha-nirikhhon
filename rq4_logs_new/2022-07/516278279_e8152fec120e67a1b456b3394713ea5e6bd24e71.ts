enum Timing {
    MORNING = 1,
    AFTERNOON = 2,
    EVENING = 4,
}

interface QRSettings {
    name: string,
    timing: Set<Timing>,
    gap: Number,
    amount: Number,
}

function generateQRData(settings: QRSettings): string {
    const name = settings.name.replace(';', '.').replace(':', '-'); // sanitize

    let timingNum = 0;
    settings.timing.forEach(timing => {
        timingNum |= timing;
    });

    return `${name}:${timingNum}:${settings.gap}:${settings.amount}`;
}

export type { QRSettings };
export { Timing, generateQRData };