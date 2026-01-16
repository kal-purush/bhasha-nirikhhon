function help(msg, botColor, prefix, type) { //first param is msg/future inter, botColor and prefix are global settings, type is either msg or inter
    const messageEmbed = {
        color: botColor,
        title: 'lodobot v21.3.7',
        description: 'Dost�pne komendy:',
        fields: [
            {
                name: prefix + 'loda?',
                value: 'spytaj si� bota czy to pora na loda ekipy'
            },
            {
                name: prefix + 'nie wiem',
                value: 'ty no nie wiem'
            },
            {
                name: prefix + 'drzwi',
                value: 'po chuj napierdalasz w te drzwi psychopatko jebana'
            },
            {
                name: prefix + 'p link do youtube/termin wyszukiwania',
                value: 'odtwarza co� lub dodaje do kolejki'
            },
            {
                name: prefix + 'q',
                value: 'wy�wietla kolejk� utwor�w'
            },
            {
                name: prefix + 'skip',
                value: 'przeskakuje do nast�pnego utworu w kolejce'
            }
        ]
    }
    msg.channel.send({ embeds: [messageEmbed] })
    return 0;
}
exports.help = help;