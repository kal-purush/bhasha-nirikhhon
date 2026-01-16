import axios from "axios";

export default class FakeYoutube {
  constructor() {}

  async search(keyword) {
    //#은 privte함수 외부에서는 사용불가. 내부에서만 사용가능
    return keyword ? this.#searchByKeword(keyword) : this.#mostPopular();
  }

  async #searchByKeword() {
    return (
      axios
        .get(`/videos/search.json`)
        .then((res) => res.data.items)
        //items 맵으로 순회하면서 우리만의 객체로 변환해줄거임 단하나 id를 객체가아니라
        //id 안에있는 videoId로 바꿔줄거임
        .then((items) => items.map({ ...items, ind: items.id.videoId }))
    );
  }

  async #mostPopular() {
    return axios.get(`/videos/search.json`).then((res) => res.data.items);
  }
}