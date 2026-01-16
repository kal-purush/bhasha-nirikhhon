export class Profile {
    gender: string;
    name: string;
    location: {
        street: string,
        city: string;
        state: string;
        postcode: number;
    };
    email: string;
    dob: string;
    picture: {
        large: string,
        medium: string,
        thumbnail: string;
    };

    public static fromJson(jsonObj: any): Profile {
        console.log(jsonObj);
        const firstPersona = jsonObj.results[0];
        const newProfile = new Profile();
        newProfile.name = firstPersona.name.first[0].toUpperCase() + firstPersona.name.first.slice(1) + ' ' +
                            firstPersona.name.last[0].toUpperCase() + firstPersona.name.last.slice(1);
        newProfile.picture = {
            large: firstPersona.picture.large,
            medium: firstPersona.picture.medium,
            thumbnail: firstPersona.picture.thumbnail
        };
        return newProfile;
    }
}