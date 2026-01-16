const btnModal = document.querySelector(".js-open-modal"); // chỉ lấy một class có tên là js-open-modal
const modal = document.querySelector(".js-modal");
const btnCloses = document.querySelectorAll(".js-close"); // lấy tất cả class là js-close cú pháp querSelectorAll
const modalInner = document.querySelector(".js-inner");
function showModal() {
	modal.classList.add("open"); // thêm clas open
}
for (const btnclose of btnCloses) {
	//tạo vòng lập nếu có nhiều nút bấm
	btnclose.addEventListener("click", hideModal); // khi click vào thằng btn có class js-close
}
function hideModal() {
	modal.classList.remove("open"); // xoá class open
}

btnModal.addEventListener("click", showModal); //lang nghe khi click vào thằng btn có class js-open-modal
modal.addEventListener("click", hideModal); // lang nghe khi click vào thằng btn có class js-modal
modalInner.addEventListener("click", function (event) {
	event.stopPropagation();
}); // lắng nghe khi click vào nội dung của modal thì ko tắt đi.