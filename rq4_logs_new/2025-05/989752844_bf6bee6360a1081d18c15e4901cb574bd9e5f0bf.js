import $ from "@js/jquery";

class AppSnackBar {
  constructor() {
    this.timeout = null;
  }

  show({ message, duration, type }) {
    this.hide();

    if (type === "error") {
      $("#app-snackbar").addClass("app-snackbar--error");
    }

    $("#app-snackbar").show();
    $(
      "#app-snackbar .app-snackbar__content .app-snackbar__content__message "
    ).text(message);

    this.timeout = setTimeout(() => {
      this.hide();
    }, duration || 3000);
  }

  hide() {
    $("#app-snackbar").hide();
    $("#app-snackbar").removeClass("app-snackbar--error");
    clearTimeout(this.timeout);
  }
}

window.appSnackBar = new AppSnackBar();