import axios from "axios";

interface IPostImgFile {
  imgFile: File | Blob;
}
export const postImg = async ({ imgFile }: IPostImgFile) => {
  const formData = new FormData();
  formData.append("multipartFile", imgFile);
  const res = await axios({
    method: "post",
    url: "https://foodreco.tk/images/post",
    data: formData,
    headers: { "Content-Type": "multipart/form-data" },
  }).then((r: any) => {
    console.log(r);
    if (r.status / 100 !== 2) {
      alert("에러입니다.");
    } else return r;
  });

  return res.data;
};