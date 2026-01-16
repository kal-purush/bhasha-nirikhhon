import { readFilePromise } from "./readFilePromise";

readFilePromise('./package.json')
  .then((content: string) => {
    console.log(content)  //package.json 파일을 읽는 내용
    return readFilePromise('./tsconfig.json');
  })
  .then((content: string) => {
    console.log(content) //tsconfig.json 파일을 읽는 내용
    /* catch 쪽 콜백 함수에 'EISDIR: illegal operation on a direcktory, read'라는 오류 메시지 전달 */
    return readFilePromise('.');
  })
  .catch((err: Error) => console.log('error:', err.message))
  .finally(()=> console.log('프로그램 종료'))