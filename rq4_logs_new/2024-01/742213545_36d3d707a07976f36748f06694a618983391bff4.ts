export function formatBrPhoneNumber(phone: string) {
  const cleaned = phone.replaceAll(/\D/g, '')
  const phoneLength = cleaned.length

  const ddd = cleaned.slice(0, 2)
  const firstPart = cleaned.slice(2, 7)
  const secondPart = cleaned.slice(7)

  if (phoneLength >= 3 && phoneLength <= 7) {
    if (phone === `(${ddd})`) {
      return phone
    } else if (phone[10] === '-') {
      return `(${ddd}) ${firstPart}-`
    } else {
      return `(${ddd}) ${firstPart}`
    }
  } else if (phoneLength > 6) {
    return `(${ddd}) ${firstPart}-${secondPart}`
  } else {
    return phone
  }
}