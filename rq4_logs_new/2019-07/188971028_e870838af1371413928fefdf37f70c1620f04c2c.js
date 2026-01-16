const path = require('path')
const { getBotConfig, genChallEmbed } = require(path.join(__dirname, '../util/util'))
const { themecolour } = getBotConfig()

const description = `
RSA... RSA...

Output of the below script is [here](https://paste.ee/r/HowMY/0).

\`\`\`python
from Crypto.Util.number import getPrime, bytes_to_long

N = getPrime(1024)*getPrime(1024) 
e = 17

M1 = bytes_to_long(b'MISCCTF{first part of the flag goes here')
M2 = bytes_to_long(b'second part is here!}')
b = M1 - M2 + getPrime(208)*N

C1 = pow(M1, e, N)
C2 = pow(M2, e,  N)
print(f'e={e}\\nN={N}\\nb={b}\\nC1={C1}\\nC2={C2}')
\`\`\``

var chall = {
    challid: 'crypto14',
    title: 'RSA Level 8',
    category: 'Cryptography',
    points: 40,
    authorid: '111028987836313600',
    authorName: 'Joseph',
    flag: 'da5f28de59da2196fe934536030365d352c4efb3b5f75c6868e70169016c5bbd',
    description,
    desc: async function(msg) {
        
        let { challid, title, category, points, authorid, authorName, description } = chall

        var descEmbed = await genChallEmbed({
            challid, title, category, points, authorid, authorName, themecolour, description
        })

        msg.channel.send({ embed: descEmbed })
    },
    notes: []
}

module.exports = chall