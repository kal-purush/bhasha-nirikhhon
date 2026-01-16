
export class Utils {
    public sanitizeEmail(email: string): string {
        email = email.trim();
        email = email.replace(/\./g, "_");
        email = email.replace(/\$/g, "_");
        email = email.replace(/\[/g, "_");
        email = email.replace(/\]/g, "_");
        email = email.replace(/#/g, "_");
        email = email.replace(/\//g, "_");
        return email;
    }

    public emailFromPhone(phone: string, attempt: number) {
        if (attempt == 0) {
            return 'user_' + phone.toString() + '@trackbabyvitals.com';
        }
        else {
            return 'user_' + phone.toString() + '_' + attempt + '@trackbabyvitals.com';
        }
    }

    generateSecretCode(): number {
        var min = 100000; var max = 999999;
        var now: number = (new Date()).getMilliseconds();
        return min + Math.round(now % (max - min) * Math.random());
    }

} 