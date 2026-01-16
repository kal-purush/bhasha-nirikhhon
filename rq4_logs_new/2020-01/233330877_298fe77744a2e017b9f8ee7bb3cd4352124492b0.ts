import { Photon } from '@prisma/photon';
import { config } from 'dotenv';

config();
const photon = new Photon();

const JP = 'JP' as 'JP';

async function main() {
  // Images

  const imgArrietty = {
    name: 'The Secret World of Arrietty Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/the-secret-world-of-arriety.jpg',
  };
  const imgCastleInTheSky = {
    name: 'Castle in the Sky Japanese Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/castle-in-the-sky.jpg',
  };
  const imgGraveOfTheFireflies = {
    name: 'Grave of the Fireflies Japanese Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/grave-of-the-fireflies.jpg',
  };
  const imgHowl = {
    name: 'Howl’s Moving Castle Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/howls-moving-castle.jpg',
  };
  const imgJoe = {
    name: 'Joe Hisaishi',
    url: 'https://storage.googleapis.com/aniarchive-people/joe-hisaishi.jpg',
  };
  const imgKiki = {
    name: 'Kiki’s Delivery Service Japanese Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/kikis-delivery-service.jpg',
  };
  const imgMononoke = {
    name: 'Princes Mononoke Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/princess-mononoke.jpg',
  };
  const imgMiyazaki = {
    name: 'Hayao Miyazaki',
    copyright: 'Jun Sato / WireImage / Getty Images',
    url: 'https://storage.googleapis.com/aniarchive-people/miyazaki-hayao.jpg',
  };
  const imgNausicaa = {
    name: 'Nausicaä of the Valley of the Wind Japanese Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/nausicaa-of-the-valley-of-the-wind.jpg',
  };
  const imgOnlyYesterday = {
    name: 'Only Yesterday Japanese Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/only-yesterday.jpg',
  };
  const imgPomPoko = {
    name: 'Pom Poko Japanese Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/pom-poko.jpg',
  };
  const imgPonyo = {
    name: 'Ponyo Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/ponyo.jpg',
  };
  const imgSpiritedAway = {
    name: 'Spirited Away Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/spirited-away.jpg',
  };
  const imgStudioGhibli = {
    name: 'Studio Ghibli',
    url: 'https://storage.googleapis.com/aniarchive-studios/studio-ghibli.jpg',
  };
  const imgTakahata = {
    name: 'Isao Takahata',
    copyright: 'Shizuo Kambayashi / AP',
    url: 'https://storage.googleapis.com/aniarchive-people/takahata-isao.jpg',
  };
  const imgTopcraft = {
    name: 'Topcraft',
    url: 'https://storage.googleapis.com/aniarchive-studios/topcraft.jpg',
  };
  const imgTotoro = {
    name: 'My Neighbor Totoro Japanese Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/my-neighbor-totoro.jpg',
  };
  const imgPorcoRosso = {
    name: 'Porco Rosso Japanese Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/porco-rosso.jpg',
  };
  const imgPrincessKaguya = {
    name: 'The Tale of the Princess Kaguya Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/the-tale-of-the-princess-kaguya.jpg',
  };
  const imgWindRises = {
    name: 'The Wind Rises Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/the-wind-rises.jpg',
  };
  const imgYamadas = {
    name: 'My Neighbors the Yamadas Theatrical Release Poster',
    url: 'https://storage.googleapis.com/aniarchive-films/my-neighbors-the-yamadas.jpg',
  };

  await Promise.all(
    [
      imgArrietty,
      imgCastleInTheSky,
      imgGraveOfTheFireflies,
      imgHowl,
      imgJoe,
      imgKiki,
      imgMiyazaki,
      imgMononoke,
      imgNausicaa,
      imgOnlyYesterday,
      imgPomPoko,
      imgPonyo,
      imgPorcoRosso,
      imgPrincessKaguya,
      imgSpiritedAway,
      imgStudioGhibli,
      imgTakahata,
      imgTopcraft,
      imgTotoro,
      imgWindRises,
      imgYamadas,
    ].map(img =>
      photon.images.upsert({
        where: { url: img.url },
        create: img,
        update: img,
      })
    )
  );

  // Studios

  const topcraft = {
    id: 'topcraft',
    country: JP,
    foundedYear: 1971,
    image: { connect: { url: imgTopcraft.url } },
    name: 'Topcraft',
  };

  const studioGhibli = {
    id: 'studio-ghibli',
    city: 'Koganei, Tokyo',
    country: JP,
    foundedYear: 1985,
    image: { connect: { url: imgStudioGhibli.url } },
    name: 'Studio Ghibli',
  };

  await Promise.all(
    [topcraft, studioGhibli].map(studio =>
      photon.studios.upsert({
        where: { id: studio.id },
        create: studio,
        update: studio,
      })
    )
  );

  // Films

  const nausicaa = {
    id: 'nausicaa-of-the-valley-of-the-wind',
    image: { connect: { url: imgNausicaa.url } },
    releaseYear: 1984,
    studio: { connect: { id: topcraft.id } },
    title: '風の谷のナウシカ',
    titleJP: '風の谷のナウシカ',
    titleEN: 'Nausicaä of the Valley of the Wind',
  };

  const castleInTheSky = {
    id: 'castle-in-the-sky',
    image: { connect: { url: imgCastleInTheSky.url } },
    releaseYear: 1986,
    studio: { connect: { id: studioGhibli.id } },
    title: '天空の城ラピュタ',
    titleJP: '天空の城ラピュタ',
    titleEN: 'Castle in the Sky',
  };

  const myNeighborTotoro = {
    id: 'my-neighbor-totoro',
    image: { connect: { url: imgTotoro.url } },
    releaseYear: 1988,
    studio: { connect: { id: studioGhibli.id } },
    title: 'となりのトトロ',
    titleJP: 'となりのトトロ',
    titleEN: 'My Neighbor Totoro',
  };

  const graveOfTheFireflies = {
    id: 'grave-of-the-fireflies',
    image: { connect: { url: imgGraveOfTheFireflies.url } },
    releaseYear: 1988,
    studio: { connect: { id: studioGhibli.id } },
    title: '火垂るの墓',
    titleJP: '火垂るの墓',
    titleEN: 'Grave of the Fireflies',
  };

  const kiki = {
    id: 'kikis-delivery-service',
    image: { connect: { url: imgKiki.url } },
    releaseYear: 1989,
    studio: { connect: { id: studioGhibli.id } },
    title: '魔女の宅急便',
    titleJP: '魔女の宅急便',
    titleEN: 'Kiki’s Delivery Service',
  };

  const onlyYesterday = {
    id: 'only-yesterday',
    image: { connect: { url: imgOnlyYesterday.url } },
    releaseYear: 1991,
    studio: { connect: { id: studioGhibli.id } },
    title: 'おもひでぽろぽろ',
    titleJP: 'おもひでぽろぽろ',
    titleEN: 'Only Yesterday',
  };

  const porcoRosso = {
    id: 'porco-rosso',
    image: { connect: { url: imgPorcoRosso.url } },
    releaseYear: 1992,
    studio: { connect: { id: studioGhibli.id } },
    title: '紅の豚',
    titleJP: '紅の豚',
    titleEN: 'Porco Rosso',
  };

  const pomPoko = {
    id: 'pom-poko',
    image: { connect: { url: imgPomPoko.url } },
    releaseYear: 1994,
    studio: { connect: { id: studioGhibli.id } },
    title: '平成狸合戦ぽんぽこ',
    titleJP: '平成狸合戦ぽんぽこ',
    titleEN: 'Pom Poko',
  };

  const princessMononoke = {
    id: 'princess-mononoke',
    image: { connect: { url: imgMononoke.url } },
    releaseYear: 1997,
    studio: { connect: { id: studioGhibli.id } },
    title: 'もののけ姫',
    titleJP: 'もののけ姫',
    titleEN: 'Princess Mononoke',
  };

  const myNeighborsTheYamadas = {
    id: 'my-neighbors-the-yamadas',
    image: { connect: { url: imgYamadas.url } },
    releaseYear: 1999,
    studio: { connect: { id: studioGhibli.id } },
    title: 'ホーホケキョとなりの山田くん',
    titleJP: 'ホーホケキョとなりの山田くん',
    titleEN: 'My Neighbors the Yamadas',
  };

  const spiritedAway = {
    id: 'spirited-away',
    image: { connect: { url: imgSpiritedAway.url } },
    releaseYear: 2001,
    studio: { connect: { id: studioGhibli.id } },
    title: '千と千尋の神隠し',
    titleJP: '千と千尋の神隠し',
    titleEN: 'Spirited Away',
  };

  const howlsMovingCastle = {
    id: 'howls-moving-castle',
    image: { connect: { url: imgHowl.url } },
    releaseYear: 2004,
    studio: { connect: { id: studioGhibli.id } },
    title: 'ハウルの動く城',
    titleJP: 'ハウルの動く城',
    titleEN: 'Howl’s Moving Castle',
  };

  const ponyo = {
    id: 'ponyo',
    image: { connect: { url: imgPonyo.url } },
    releaseYear: 2008,
    studio: { connect: { id: studioGhibli.id } },
    title: '崖の上のポニョ',
    titleJP: '崖の上のポニョ',
    titleEN: 'Ponyo',
  };

  const arrietty = {
    id: 'the-secret-world-of-arriety',
    image: { connect: { url: imgArrietty.url } },
    releaseYear: 2010,
    studio: { connect: { id: studioGhibli.id } },
    title: '借りぐらしのアリエッティ',
    titleJP: '借りぐらしのアリエッティ',
    titleEN: 'The Secret World of Arrietty',
  };

  const theWindRises = {
    id: 'the-wind-rises',
    image: { connect: { url: imgWindRises.url } },
    releaseYear: 2013,
    studio: { connect: { id: studioGhibli.id } },
    title: '風立ちぬ',
    titleJP: '風立ちぬ',
    titleEN: 'The Wind Rises',
  };

  const princessKaguya = {
    id: 'the-tale-of-the-princess-kaguya',
    image: { connect: { url: imgPrincessKaguya.url } },
    releaseYear: 2013,
    studio: { connect: { id: studioGhibli.id } },
    title: 'かぐや姫の物語',
    titleJP: 'かぐや姫の物語',
    titleEN: 'The Tale of the Princess Kaguya',
  };

  await Promise.all(
    [
      nausicaa,
      castleInTheSky,
      myNeighborTotoro,
      graveOfTheFireflies,
      kiki,
      onlyYesterday,
      porcoRosso,
      pomPoko,
      princessMononoke,
      myNeighborsTheYamadas,
      spiritedAway,
      howlsMovingCastle,
      ponyo,
      arrietty,
      theWindRises,
      princessKaguya,
    ].map(film =>
      photon.films.upsert({
        where: { id: film.id },
        create: film,
        update: film,
      })
    )
  );

  // People

  const miyazakiHayao = {
    id: 'miyazaki-hayao',
    birthDay: 5,
    birthMonth: 1,
    birthYear: 1941,
    country: JP,
    directed: {
      connect: [
        { id: nausicaa.id },
        { id: castleInTheSky.id },
        { id: myNeighborTotoro.id },
        { id: kiki.id },
        { id: porcoRosso.id },
        { id: princessMononoke.id },
        { id: spiritedAway.id },
        { id: howlsMovingCastle.id },
        { id: ponyo.id },
        { id: arrietty.id },
        { id: theWindRises.id },
      ],
    },
    firstName: 'Hayao',
    founded: { connect: [{ id: studioGhibli.id }] },
    image: { connect: { url: imgMiyazaki.url } },
    kanji: '宮崎駿',
    lastName: 'Miyazaki',
  };

  const takahataIsao = {
    id: 'takahata-isao',
    birthDay: 29,
    birthMonth: 10,
    birthYear: 1935,
    country: JP,
    deathDay: 5,
    deathMonth: 4,
    deathYear: 2018,
    directed: {
      connect: [
        { id: graveOfTheFireflies.id },
        { id: onlyYesterday.id },
        { id: pomPoko.id },
        { id: myNeighborsTheYamadas.id },
        { id: princessKaguya.id },
      ],
    },
    firstName: 'Isao',
    image: { connect: { url: imgTakahata.url } },
    kanji: '高畑勲',
    lastName: 'Takahata',
  };

  const joeHisaishi = {
    id: 'joe-hisaishi',
    alias: 'Joe Hisaishi',
    birthDay: 6,
    birthMonth: 12,
    birthYear: 1950,
    country: JP,
    composed: {
      connect: [
        { id: nausicaa.id },
        { id: castleInTheSky.id },
        { id: myNeighborTotoro.id },
        { id: kiki.id },
        { id: porcoRosso.id },
        { id: princessMononoke.id },
        { id: spiritedAway.id },
        { id: howlsMovingCastle.id },
        { id: ponyo.id },
        { id: princessKaguya.id },
      ],
    },
    firstName: 'Mamoru',
    image: { connect: { url: imgJoe.url } },
    kanji: '藤澤守',
    lastName: 'Fujisawa',
  };

  await Promise.all(
    [miyazakiHayao, takahataIsao, joeHisaishi].map(person =>
      photon.people.upsert({
        where: { id: person.id },
        create: person,
        update: person,
      })
    )
  );
}

main()
  .catch(e => console.error(e))
  .finally(async () => {
    await photon.disconnect();
  });