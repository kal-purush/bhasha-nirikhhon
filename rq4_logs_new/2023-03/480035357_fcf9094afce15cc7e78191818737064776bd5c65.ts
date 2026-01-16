import { watch, nextTick, onBeforeUnmount, onMounted } from "vue";
import type { Text } from "@darwin-studio/vue-ui/src/types/text";
import { EVENT_NAME } from "@darwin-studio/vue-ui/src/constants/event-name";
import useSetBodyOverflow from "./set-body-overflow";
import useControlId from "./control-id";

// TODO: tests

/**
 * Blocks body scroll, move focus to the props.focusId (or close button by default) and emits 'close' event by 'Escape' key.
 *
 * @param props
 * @param emit
 * @returns {{focusControlId: string}}
 */
export default function useClosable(
  props: {
    isShown?: boolean;
    focusId?: Text;
    whenClose?: () => void;
  },
  emit: (eventName: "close") => void
) {
  const { controlId: focusControlId } = useControlId({
    ...props,
    id: props.focusId,
  });
  const { setBodyOverflow } = useSetBodyOverflow();
  let activeElement: Element | HTMLElement | null = null;
  let keyupHandler: ((event: KeyboardEvent) => void) | null = null;

  onMounted(() => {
    if (typeof window !== "undefined") {
      keyupHandler = (event: KeyboardEvent) => {
        console.log("keyup");
        if (event.key === "Escape") {
          emit("close");
          props.whenClose?.();
        }
      };

      // use passive mode to notify browser that it can perform default action immediately
      window.addEventListener(EVENT_NAME.KEYUP, keyupHandler, {
        passive: true,
      });
    }
  });

  watch(
    () => props.isShown,
    async (isShown) => {
      if (isShown) {
        setBodyOverflow();
        activeElement = document.activeElement;
        await nextTick(() => {
          document.getElementById(focusControlId.value)?.focus?.();
        });
      } else {
        setBodyOverflow(false);
        (activeElement as HTMLElement)?.focus?.();
      }
    }
  );

  onBeforeUnmount(() => {
    // ensure that body scrolling isn't blocked
    setBodyOverflow(false);

    if (typeof window !== "undefined" && keyupHandler) {
      window.removeEventListener(EVENT_NAME.KEYUP, keyupHandler);
    }
  });

  return { focusControlId };
}