const projects = {
  bul: {
    artist: 'Joana Bachurova',
    faculty: 'Fakultät Musik',
    language: 'Bulgarian',
    subject: 'Lehramt Musik an Grundschulen',
  },
  deu: {
    artist: 'Aaron Langguth',
    faculty: 'Fakultät Gestaltung',
    language: 'German',
    subject: 'Gesellschafts- und Wirtschaftskommunikation',
  },
  hin: {
    artist: 'Agneya Chikte',
    faculty: 'Jazz Institut Berlin',
    language: 'Hindi',
    subject: 'Master of Music',
  },
  jpn: {
    artist: 'Yui Yamagishi',
    faculty: 'Fakultät Gestaltung',
    language: 'Japanese',
    subject: 'Visuelle Kommunikation',
  },
  kat: {
    artist: 'Liza Bokeria',
    faculty: 'Fakultät Gestaltung',
    language: 'Georgian',
    subject: 'Architektur',
  },
  mar: {
    artist: 'Agneya Chikte',
    faculty: 'Jazz Institut Berlin',
    language: 'Marathi',
    subject: 'Master of Music',
  },
  por: {
    artist: 'Kim R. Martins',
    faculty: 'Berlin Career College',
    language: 'Portugese (Brazilian)',
    subject: 'Sound Studies and Sonic Arts',
  },
  rus: {
    artist: 'Konstantin Maltcev',
    faculty: 'Fakultät Musik',
    language: 'Russian',
    subject: 'Lehramt Musik',
  },
  spa: {
    artist: 'B. Skarlett B. Cisneros',
    faculty: 'Fakultät Gestaltung',
    language: 'Spanish',
    subject: 'Produktdesign',
  },
  srp: {
    artist: 'Milica Milić',
    faculty: 'Fakultät Darstellende Kunst',
    language: 'Serbian',
    subject: 'Operngesang und Musiktheater',
  },
  ukr: {
    artist: 'Angelina Cherny',
    faculty: 'Fakultät Gestaltung',
    language: 'Ukrainian',
    subject: 'Visuelle Kommunikation',
  },
  urd: {
    artist: 'Hira Noor',
    faculty: 'Fakultät Bildende Kunst',
    language: 'Urdu',
    subject: 'Art in Context',
  },
  yue: {
    artist: 'Peony Lo',
    faculty: 'Fakultät Bildende Kunst',
    language: 'Cantonese',
    subject: 'Bildende Kunst',
  },
  zho: {
    artist: 'Yuxiao Feng',
    faculty: 'Fakultät Gestaltung',
    language: 'Chinese',
    subject: 'Architektur',
  },
};

// Use the default: `always`
export const localePrefix = undefined;

export type ProjectLanguages = keyof typeof projects;
export default projects;