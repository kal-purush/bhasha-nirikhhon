export function handleRedirectToWhatsApp(whatsapp: string): void {
  const customUrl = `https://api.whatsapp.com/send/?phone=55${whatsapp}`

  window.open(customUrl)
}

export function handleRedirectToEmail(email: string): void {
  const customUrl = `mailto:${email}`

  window.open(customUrl)
}