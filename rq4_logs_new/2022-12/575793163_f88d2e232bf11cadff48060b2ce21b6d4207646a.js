class Loader {
  loading() {
    let button =
      document.querySelector("#login-button") ||
      document.querySelector("#register-button");

    if (button) {
      button.addEventListener("click", () => {
        document.querySelector(".lds-spinner-container").style.visibility =
          " visible";
      });
    }
  }
}

export default Loader;