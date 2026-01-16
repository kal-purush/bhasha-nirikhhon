import { Profile } from '../../classes/profile';
export const ProfileMockData: Profile[] = [
    {
        "gender": "male",
        "name": "Josh van Bodegom",
        "location": {
            "street": "Willem van Noortplein 6",
            "city": "Utrecht",
            "state": "Utrecht",
            "postcode": "3514GK"
        },
        "email": "joshi.vanbodegom@example.com",
        "login": {
            "username": "josh",
            "password": "wachtwoord123"
        },
        "dob": "1981-06-20 00:28:43",
        "phone": "06-17871651",
        "picture": {
            "large": "https://randomuser.me/api/portraits/men/49.jpg",
            "medium": "https://randomuser.me/api/portraits/med/men/49.jpg",
            "thumbnail": "https://randomuser.me/api/portraits/thumb/men/49.jpg"
        }
    },
    {
        "gender": "female",
        "name": "Chantal de Vries",
        "location": {
            "street": "Liedeweg 14",
            "city": "Haarlemmerliede",
            "state": "Noord-Holland",
            "postcode": "2065AH"
        },
        "email": "chantal.devries@example.com",
        "login": {
            "username": "chantal",
            "password": "wachtwoord123"
        },
        "dob": "1981-08-11 00:26:42",
        "phone": "06-01965711",
        "picture": {
            "large": "https://randomuser.me/api/portraits/women/65.jpg",
            "medium": "https://randomuser.me/api/portraits/med/women/65.jpg",
            "thumbnail": "https://randomuser.me/api/portraits/thumb/women/65.jpg"
        }
    },
    {
        "gender": "female",
        "name": "Jinji van Doorn",
        "location": {
            "street": "Beeningerstraat 47-B",
            "city": "Rotterdam",
            "state": "Zuid-Holland",
            "postcode": "3042TE"
        },
        "email": "jinji.vandoorn@example.com",
        "login": {
            "username": "jinji",
            "password": "wachtwoord123"
        },
        "dob": "1987-04-24 12:02:26",
        "phone": "06-27891471",
        "picture": {
            "large": "https://randomuser.me/api/portraits/women/51.jpg",
            "medium": "https://randomuser.me/api/portraits/med/women/51.jpg",
            "thumbnail": "https://randomuser.me/api/portraits/thumb/women/51.jpg"
        },
    }
];
