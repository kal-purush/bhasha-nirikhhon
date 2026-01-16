const $element = document.getElementById("data")
const showData = data => {
  const ip = data.ip
  $element.innerText = ip
}
const showError = err => {
  $element.innerHTML = "Problem while fetching your IP address"
}
fetch("https://freegeoip.net/json/")
  .then(response => response.json())
  .then(showData)
  .catch(showError)