import { atom } from "recoil";
import { Data } from "@/modules/MyStudy/type";
import Icon from "@/assets/myStudy/mahasiswa.svg";
import Icon2 from "@/assets/myStudy/matkul.svg";
import Icon3 from "@/assets/myStudy/sks.svg";
import Icon4 from "@/assets/myStudy/semester.svg";

export const dataCardState = atom<Array<Data>>({
  key: "option-faculty-state",
  default: [
    { icon: Icon, jumlah: "-", detail: "mahasiswa", warna: "#E9F6FD" },
    { icon: Icon2, jumlah: "-", detail: "matkul", warna: "#E3FBDA" },
    { icon: Icon3, jumlah: "-", detail: "sks", warna: "#FEF6D0" },
    { icon: Icon4, jumlah: "-", detail: "semester", warna: "#E9F6FD" },
  ],
});