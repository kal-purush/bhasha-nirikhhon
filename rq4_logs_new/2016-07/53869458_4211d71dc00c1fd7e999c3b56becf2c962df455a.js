/* global formatPhone:true */
/* exported formatPhone */

formatPhone = phone => {
  if (!phone) {
    return null
  }
  const phoneDigits = phone.toString().replace(/\D/g, '')
  const areaCode = phoneDigits.slice(0, 3)
  const prefix = phoneDigits.slice(3, 6)
  const suffix = phoneDigits.slice(6, 10)
  let formatted = String(areaCode)
  if (phoneDigits.length > 3) {
    formatted = `(${areaCode}) ${prefix}`
  }
  if (phoneDigits.length > 6) {
    formatted += `-${suffix}`
  }
  return formatted
}