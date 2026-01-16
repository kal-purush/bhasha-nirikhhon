var map = new Map<string, Map<string, string>>([[
    "NL", new Map([
        ["login_username", "Gebruikersnaam"],
        ["login_password", "Wachtwoord"],
        ["login_send", "Inloggen"],
        ["login_reset", "Wachtwoord vergeten?"],
        ["login_invalid", "Inloggen mislukt! Probeer het nogmaals!"]
    ])],[
    "EN", new Map([
        ["login_username", "Username"],
        ["login_password", "Password"],
        ["login_send", "Login"],
        ["login_reset", "Forgot password?"],
        ["login_invalid", "Login failed! Please try again!"]
    ])]
])

export default class Lang{
    private static language:string = "NL";    

    public static get(tag:string){
        return map.get(this.language).get(tag);
    }
}