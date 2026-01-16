import ChatForm from "./chat-form";
import { validateNotEmpty } from "../../utils/validate";
import ChatController from "../../system/controllers/chat-controller/chat-controller";

const chatController = new ChatController();

function submitHandler(evt: SubmitEvent) {
  evt.preventDefault();
  const formData = new FormData(evt.target as HTMLFormElement);
  const data = Object.fromEntries(formData.entries());
  if (evt && evt.target) {
    if (validateNotEmpty(data.message as string)) {
      const form = evt?.target as HTMLFormElement;
      if (form) {
        const input = form.querySelector(`input`);
        if (input) {
          // console.info(input.value, this.props.activeChatId);
          chatController.sendMessage(
            data.message as string,
            this.props.activeChatId
          );
          input.value = ``;
        }
      }
    } else {
      console.error(`Empty message not allowed!`);
    }
  }
}

export const chatForm = new ChatForm({
  placeHolder: `Message`,
  events: { submit: [submitHandler] },
  attrs: {
    class: `chat-text-block__form message-form`,
  },
});