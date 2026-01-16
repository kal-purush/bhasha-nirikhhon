import { fromEvent } from "rxjs";
import getDataFromStorage from "./getDataFromStorage";
import state from "../type/state";
import createIssue from "./createIssue";

export default function addIssueCumstomBtn(
  newIssueBtn: HTMLAnchorElement
): void {
  const customIssueBtn = document.createElement("a");

  customIssueBtn.innerText = "Custom issue";

  [...newIssueBtn.classList].forEach(className => {
    customIssueBtn.classList.add(className);
  });

  customIssueBtn.classList.add("custom-btn");

  newIssueBtn.parentNode!.insertBefore(customIssueBtn, newIssueBtn.nextSibling);

  fromEvent(customIssueBtn, "click").subscribe(() => {
    getDataFromStorage()
      .then((res: state): Promise<Response> => createIssue(res))
      .then((res: Response): Promise<any> => res.json())
      .then(
        (res: any): void => {
          window.location.href = res.html_url;
        }
      );
  });
}