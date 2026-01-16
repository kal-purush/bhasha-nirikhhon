type IndustryCode =
  | number
  | "X" // used for "Other industries"
  | "A"
  | "F"
  | "H"
  | "J"
  | "L"
  | "M"
  | "P"
  | "Q"
  | "R"
  | "S";

interface Industry {
  code: IndustryCode;
  parentNames: {
    fi: string;
    en: string;
  };
  names: {
    fi: string;
    en: string;
  };
  subIndustries: SubIndustry[];
}

interface SubIndustry {
  code: IndustryCode;
  names: {
    fi: string;
    en: string;
  };
}

const unsortedIndustries: Industry[] = [
  {
    code: 55,
    parentNames: {
      fi: "Majoitus- ja ravitsemistoiminta",
      en: "Accommodation and food service activities",
    },
    names: {
      fi: "Majoitus",
      en: "Accommodation",
    },
    subIndustries: [
      {
        code: 5510,
        names: {
          fi: "Hotellit ja vastaavat majoitusliikkeet",
          en: "Hotels and similar accommodation",
        },
      },
      {
        code: 5520,
        names: {
          fi: "Lomakylät, retkeilymajat yms. majoitus",
          en: "Holiday and other short-stay accommodation",
        },
      },
      {
        code: 5530,
        names: {
          fi: "Leirintäalueet, asuntovaunu- ja matkailuvaunualueet",
          en: "Camping grounds, recreational vehicle parks and trailer parks",
        },
      },
      {
        code: 5590,
        names: {
          fi: "Muu majoitus",
          en: "Other accommodation",
        },
      },
    ],
  },
  {
    code: 56,
    parentNames: {
      fi: "Majoitus- ja ravitsemistoiminta",
      en: "Accommodation and food service activities",
    },
    names: {
      fi: "Ravitsemistoiminta",
      en: "Food and beverage service activities",
    },
    subIndustries: [
      {
        code: 5610,
        names: {
          fi: "Ravintolat ja vastaava ravitsemistoiminta",
          en: "Restaurants and mobile food service activities",
        },
      },
      {
        code: 5629,
        names: {
          fi: "Henkilöstö- ja laitosruokalat",
          en: "Other food service activities",
        },
      },
      {
        code: 5630,
        names: {
          fi: "Baarit ja kahvilat",
          en: "Beverage serving activities",
        },
      },
    ],
  },
  {
    code: 68,
    parentNames: {
      fi: "Kiinteistöalan toiminta",
      en: "Real estate activities",
    },
    names: {
      fi: "Kiinteistöalan toiminta",
      en: "Real estate activities",
    },
    subIndustries: [
      {
        code: 6810,
        names: {
          fi: "Omien kiinteistöjen kauppa",
          en: "Buying and selling of own real estate",
        },
      },
      {
        code: 6820,
        names: {
          fi: "Omien tai leasing-kiinteistöjen vuokraus ja hallinta",
          en: "Renting and operating of own or leased real estate",
        },
      },
      {
        code: 683,
        names: {
          fi: "Kiinteistöalan toiminta palkkio- tai sopimusperusteisesti",
          en: "Real estate activities on a fee or contract basis",
        },
      },
    ],
  },
  {
    code: 49,
    parentNames: {
      fi: "Kuljetus ja varastointi",
      en: "Transportation and storage",
    },
    names: {
      fi: "Maaliikenne ja putkijohtokuljetus",
      en: "Land transport and transport via pipelines",
    },
    subIndustries: [
      {
        code: 4910,
        names: {
          fi: "Rautateiden henkilöliikenne, kaukoliikenne",
          en: "Passenger rail transport, interurban",
        },
      },
      {
        code: 4932,
        names: {
          fi: "Taksiliikenne",
          en: "Taxi operation",
        },
      },
      {
        code: 4939,
        names: {
          fi: "Muualla luokittelematon maaliikenteen henkilöliikenne",
          en: "Other passenger land transport n.e.c.",
        },
      },
    ],
  },
  {
    code: 50,
    parentNames: {
      fi: "Kuljetus ja varastointi",
      en: "Transportation and storage",
    },
    names: {
      fi: "Vesiliikenne",
      en: "Water transport",
    },
    subIndustries: [
      {
        code: 5010,
        names: {
          fi: "Meri- ja rannikkovesiliikenteen henkilökuljetus",
          en: "Sea and coastal passenger water transport",
        },
      },
      {
        code: 5030,
        names: {
          fi: "Sisävesiliikenteen henkilökuljetus",
          en: "Inland passenger water transport",
        },
      },
    ],
  },
  {
    code: 51,
    parentNames: {
      fi: "Kuljetus ja varastointi",
      en: "Transportation and storage",
    },
    names: {
      fi: "Ilmaliikenne",
      en: "Air transport",
    },
    subIndustries: [
      {
        code: 5110,
        names: {
          fi: "Matkustajalentoliikenne",
          en: "Passenger air transport",
        },
      },
    ],
  },
  {
    code: 77,
    parentNames: {
      fi: "Hallinto- ja tukipalvelutoiminta",
      en: "Administrative and support service activities",
    },
    names: {
      fi: "Vuokraus- ja leasingtoiminta",
      en: "Rental and leasing activities",
    },
    subIndustries: [
      {
        code: 7711,
        names: {
          fi: "Autojen ja kevyiden moottoriajoneuvojen vuokraus ja leasing",
          en: "Renting and leasing of cars and light motor vehicles",
        },
      },
      {
        code: 7721,
        names: {
          fi: "Vapaa-ajan ja urheiluvälineiden vuokraus ja leasing",
          en: "Renting and leasing of recreational and sports goods",
        },
      },
    ],
  },
  {
    code: 79,
    parentNames: {
      fi: "Hallinto- ja tukipalvelutoiminta",
      en: "Administrative and support service activities",
    },
    names: {
      fi: "Matkatoimistojen ja matkanjärjestäjien toiminta; varauspalvelut",
      en: "Travel agency, tour operator and other reservation service and related activities",
    },
    subIndustries: [
      {
        code: 7911,
        names: {
          fi: "Matkatoimistojen toiminta",
          en: "Travel agency activities",
        },
      },
      {
        code: 7912,
        names: {
          fi: "Matkanjärjestäjien toiminta",
          en: "Tour operator activities",
        },
      },
      {
        code: 7990,
        names: {
          fi: "Varauspalvelut, matkaoppaiden palvelut ym.",
          en: "Other reservation service and related activities",
        },
      },
    ],
  },
  {
    code: 90,
    parentNames: {
      fi: "Taiteet, viihde ja virkistys",
      en: "Arts, entertainment and recreation",
    },
    names: {
      fi: "Kulttuuri- ja viihdetoiminta",
      en: "Creative, arts and entertainment activities",
    },
    subIndustries: [
      {
        code: 900,
        names: {
          fi: "Kulttuuri- ja viihdetoiminta",
          en: "Creative, arts and entertainment activities",
        },
      },
    ],
  },
  {
    code: 91,
    parentNames: {
      fi: "Taiteet, viihde ja virkistys",
      en: "Arts, entertainment and recreation",
    },
    names: {
      fi: "Kirjastojen, arkistojen, museoiden ja muiden kulttuurilaitosten toiminta",
      en: "Libraries, archives, museums and other cultural activities",
    },
    subIndustries: [
      {
        code: 9102,
        names: {
          fi: "Museoiden toiminta",
          en: "Museums activities",
        },
      },
      {
        code: 9103,
        names: {
          fi: "Historiallisten nähtävyyksien, rakennusten ja vastaavien kohteiden toiminta",
          en: "Operation of historical sites and buildings and similar visitor attractions",
        },
      },
      {
        code: 9104,
        names: {
          fi: "Kasvitieteellisten puutarhojen, eläintarhojen ja luonnonpuistojen toiminta",
          en: "Botanical and zoological gardens and nature reserves activities",
        },
      },
    ],
  },
  {
    code: 92,
    parentNames: {
      fi: "Taiteet, viihde ja virkistys",
      en: "Arts, entertainment and recreation",
    },
    names: {
      fi: "Rahapeli- ja vedonlyöntipalvelut",
      en: "Gambling and betting activities",
    },
    subIndustries: [
      {
        code: 9200,
        names: {
          fi: "Rahapeli- ja vedonlyöntipalvelut",
          en: "Gambling and betting activities",
        },
      },
    ],
  },
  {
    code: 93,
    parentNames: {
      fi: "Taiteet, viihde ja virkistys",
      en: "Arts, entertainment and recreation",
    },
    names: {
      fi: "Urheilutoiminta sekä huvi- ja virkistyspalvelut",
      en: "Sports activities and amusement and recreation activities",
    },
    subIndustries: [
      {
        code: 9311,
        names: {
          fi: "Urheilulaitosten toiminta",
          en: "Operation of sports facilities",
        },
      },
      {
        code: 9319,
        names: {
          fi: "Muu urheilutoiminta",
          en: "Other sports activities",
        },
      },
      {
        code: 9321,
        names: {
          fi: "Huvi- ja teemapuistojen toiminta",
          en: "Activities of amusement parks and theme parks",
        },
      },
      {
        code: 9329,
        names: {
          fi: "Muu huvi- ja virkistystoiminta",
          en: "Other amusement and recreation activities",
        },
      },
    ],
  },
  {
    code: 46,
    parentNames: {
      fi: "Tukku- ja vähittäiskauppa; moottoriajoneuvojen ja moottoripyörien korjaus",
      en: "Wholesale and retail trade; repair of motor vehicles and motorcycles",
    },
    names: {
      fi: "Tukkukauppa (pl. moottoriajoneuvojen ja moottoripyörien kauppa)",
      en: "Wholesale trade, except of motor vehicles and motorcycles",
    },
    subIndustries: [
      {
        code: 46,
        names: {
          fi: "Tukkukauppa",
          en: "Wholesale trade",
        },
      },
    ],
  },
  {
    code: 47,
    parentNames: {
      fi: "Tukku- ja vähittäiskauppa; moottoriajoneuvojen ja moottoripyörien korjaus",
      en: "Wholesale and retail trade; repair of motor vehicles and motorcycles",
    },
    names: {
      fi: "Vähittäiskauppa (pl. moottoriajoneuvojen ja moottoripyörien kauppa)",
      en: "Retail trade, except of motor vehicles and motorcycles",
    },
    subIndustries: [
      {
        code: 47,
        names: {
          fi: "Vähittäiskauppa",
          en: "Retail trade",
        },
      },
    ],
  },
  {
    code: "X",
    parentNames: {
      fi: "Muu toimialat",
      en: "Other industries",
    },
    names: {
      fi: "Muu toimialat",
      en: "Other industries",
    },
    subIndustries: [
      {
        code: "A",
        names: {
          fi: "Maatalous, metsätalous ja kalatalous",
          en: "Agriculture, forestry and fishing",
        },
      },
      {
        code: "F",
        names: {
          fi: "Rakentaminen",
          en: "Construction",
        },
      },
      {
        code: "H",
        names: {
          fi: "Kuljetus ja varastointi",
          en: "Construction",
        },
      },
      {
        code: "J",
        names: {
          fi: "Informaatio ja viestintä",
          en: "Information and communication",
        },
      },
      {
        code: "L",
        names: {
          fi: "Kiinteistöalan toiminta",
          en: "Real estate activities",
        },
      },
      {
        code: "M",
        names: {
          fi: "Ammatillinen, tieteellinen ja tekninen toiminta",
          en: "Professional, scientific and technical activities",
        },
      },
      {
        code: "P",
        names: {
          fi: "Koulutus",
          en: "Education",
        },
      },
      {
        code: "Q",
        names: {
          fi: "Terveys- ja sosiaalipalvelut",
          en: "Human health and social work activities",
        },
      },
      {
        code: "R",
        names: {
          fi: "Taiteet, viihde ja virkistys",
          en: "Arts, entertainment and recreation",
        },
      },
      {
        code: "S",
        names: {
          fi: "Muu palvelutoiminta",
          en: "Other service activities",
        },
      },
    ],
  },
];