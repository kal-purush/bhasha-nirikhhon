import { computed, Ref, ref, watch, SetupContext } from "vue";
import {
  EventLocationFormOption,
  UPDATE_MODEL_VALUE
} from "@/commons/constant";
import { EventLocationForm } from "@/commons/Interfaces";

export default function useEventLocationForm(
  initialEventLocation: Ref<EventLocationForm>,
  context: SetupContext<"update:modelValue"[]>
) {
  const currentOption: Ref<EventLocationFormOption> = ref(
    EventLocationFormOption.SPECIFY
  );

  const specifyEventLocation: Ref<EventLocationForm> = ref({
    name: initialEventLocation.value.name,
    description: initialEventLocation.value.description,
    googleMapUrl: initialEventLocation.value.googleMapUrl,
    isOnline: initialEventLocation.value.isOnline,
    isValid: initialEventLocation.value.isValid
  });

  const laterEventLocation: EventLocationForm = {
    name: "Announce later",
    description: "",
    googleMapUrl: "https://www.onepass.app/",
    isOnline: false,
    isValid: true
  };

  const onlineEventLocation: EventLocationForm = {
    name: "Online",
    description: "",
    googleMapUrl: "https://www.onepass.app/",
    isOnline: true,
    isValid: true
  };

  const isSpecifyOption = computed(() => {
    return currentOption.value === EventLocationFormOption.SPECIFY;
  });

  const isLaterOption = computed(() => {
    return currentOption.value === EventLocationFormOption.LATER;
  });

  const isOnlineOption = computed(() => {
    return currentOption.value === EventLocationFormOption.ONLINE;
  });

  function toSpecifyOption() {
    currentOption.value = EventLocationFormOption.SPECIFY;
  }

  function toLaterOption() {
    currentOption.value = EventLocationFormOption.LATER;
  }

  function toOnlineOption() {
    currentOption.value = EventLocationFormOption.ONLINE;
  }

  watch(
    () => specifyEventLocation,
    () => context.emit(UPDATE_MODEL_VALUE, specifyEventLocation.value),
    { deep: true }
  );

  watch(currentOption, () => {
    if (currentOption.value === EventLocationFormOption.SPECIFY)
      context.emit(UPDATE_MODEL_VALUE, specifyEventLocation.value);
    else if (currentOption.value === EventLocationFormOption.LATER)
      context.emit(UPDATE_MODEL_VALUE, laterEventLocation);
    else context.emit(UPDATE_MODEL_VALUE, onlineEventLocation);
  });

  return {
    specifyEventLocation,
    isSpecifyOption,
    isLaterOption,
    isOnlineOption,
    toSpecifyOption,
    toLaterOption,
    toOnlineOption
  };
}