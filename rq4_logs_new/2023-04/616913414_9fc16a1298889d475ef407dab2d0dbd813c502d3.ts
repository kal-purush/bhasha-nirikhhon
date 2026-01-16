import { atom, selectorFamily } from "recoil";
import { DictionaryType } from "./type";

export const GuideDictionaryState = atom<Array<DictionaryType>>({
  key: "option-dictionary-state",
  default: [
    {
      id: "1",
      title: "KRS",
      description:
        "Bagi beberapa maba,  istilah yang satu ini akan terdengar asing karena saat duduk di bangku sekolah menengah atau akhir, sistem KRS belum diterapkan sehingga ketika masuk lingkungan kuliah, kamu belum mengerti tentang sistem ini. Lalu, apa sih KRS itu? KRS adalah sebuah kartu yang biasanya kamu dapatkan ketika mengikuti kuliah pertama di awal semester. Tujuannya sebagai rencana pengambilan mata kuliah berdasarkan jurusan yang telah kamu ambil. Di dalam KRS, terdapat data yang perlu kamu isi, seperti nama mahasiswa, nomor induk mahasiswa, fakultas, semester saat ini, foto mahasiswa, jurusan, cap universitas, tanda tangan birokrasi atau dosen wali, dan tanda tangan kamu sendiri. Kalau kamu gak isi KRS, data kamu gak akan masuk database di kampus sehingga kamu akan dianggap cuti. Dampaknya? Kamu jadi gak bisa mengikuti kegiatan perkuliahan dan terancam terlambat lulus",
      slug: "krs",
    },
    {
      id: "2",
      title: "SKS",
      description:
        "Nilai keseluruhan mahasiswa dari beban studi yang ia ambil. IPK digunakan sebagai predikat kelulusan mahasiswa.",
      slug: "sks",
    },
    {
      id: "3",
      title: "IPK",
      description:
        "Merupakan kumpulan definisi singkat dari istilah dan akronim yang digunakan di Mobile Aplikasi Kampus Gratis.",
      slug: "ipk",
    },
    {
      id: "4",
      title: "Program Studi",
      description:
        "Kesatuan rencana belajar sebagai pedoman penyelenggaraan pendidikan akademik dan atau profesional atas dasar suatu kurikulum.",
      slug: "program-studi",
    },
    {
      id: "5",
      title: "IP",
      description:
        "Adalah singkatan dari “Indeks Prestasi,” yang artinya sama aja kayak nilai akhir semester.",
      slug: "indeks-prestasi",
    },
    {
      id: "6",
      title: "Dosen",
      description:
        "Tenaga pengajar di perguruan tinggi dengan tugas utama mengajar, membimbing, dan atau melatih mahasiswa.",
      slug: "dosen",
    },
  ],
});

//search
export const queryOptionDictionary = atom({
  key: "query-option-dictionary",
  default: "",
});

export const filterOptionDictionary = selectorFamily({
  key: "filter-option-dictionary",
  get:
    (query: string) =>
    ({ get }) =>
      get(GuideDictionaryState).filter((item) => (item.slug as unknown as string).includes(query)),
});