declare const domain: string;

export function fix_avatar_template(avatar_url) {
    // var avatar_url = avatar_url.format(size='20')
    if (avatar_url.startsWith('https')===false) {
        avatar_url = (`${domain}` + avatar_url)
    }
    return avatar_url
}