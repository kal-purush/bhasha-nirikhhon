class Modal {
  isDisplayed = false;
  modalContainer;
  constructor() {
    this.modalContainer = document.querySelector(".modal-container");
    this.hideModalBtn = document.querySelector(".modal-hide");
    this.acceptBtn = document.querySelector(".accept-btn");
    this.refuseBtn = document.querySelector(".refuse-btn");

    this.hideModalBtn.addEventListener("click", this.hide.bind(this));
    this.acceptBtn.addEventListener(
      "click",
      this.handleDeleteAccount.bind(this)
    );
    this.refuseBtn.addEventListener("click", this.hide.bind(this));
  }

  display() {
    this.isDisplayed = true;
    this.modalContainer.style.display = "block";
  }

  hide() {
    this.isDisplayed = false;
    this.modalContainer.style.display = "none";
  }

  handleDeleteAccount() {
    const modal = document.querySelector(".modal");
    modal.innerHTML = `<form method="post" action="/users/delete"><input type="password" name="password" placeholder="패스워드를 입력하세요."/><input type="submit" value="삭제"/></form>`;
  }
}

export default Modal;