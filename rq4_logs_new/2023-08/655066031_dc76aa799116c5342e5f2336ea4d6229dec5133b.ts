import { restUrl } from "src/utils";

let timeout: any;
const emailRegex = /^[\w-.]+@([\w-]+\.)+[\w-]{2,4}$/;

function emailIsValid(email: string, setError: React.Dispatch<React.SetStateAction<string>>, usedToLogin: boolean = false) {
  if (!emailRegex.test(email)) {
    setError('This is not an email');
  } else {
    setError('Checking email');

    clearTimeout(timeout);

    timeout = setTimeout(() => {
      const params = new URLSearchParams();
      params.append('email', email);
      fetch(`${restUrl}/isUsed/email?${params.toString()}`).then(response => response.json())
        .then((result: any) => {
          if (!result.res === usedToLogin) {
            if (usedToLogin) {
              setError('This email do not exists!');
            } else {
              setError('This email is used!');
            }
          } else {
            setError('');
          }
        });
    }, 300);
  }
}

export { emailIsValid };
export default emailIsValid;