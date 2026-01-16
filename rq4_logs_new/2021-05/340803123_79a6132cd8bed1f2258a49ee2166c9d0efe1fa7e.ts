import { computed, Ref, ref, watch, SetupContext } from "vue";
import {
  EventDateTimeFormOption,
  UPDATE_MODEL_VALUE
} from "@/commons/constant";
import { EventDurationsForm } from "@/commons/Interfaces";

export default function useEventDateTimeForm(
  initialEventDurations: Ref<EventDurationsForm>,
  context: SetupContext<"update:modelValue"[]>
) {
  const currentOption: Ref<EventDateTimeFormOption> = ref(
    EventDateTimeFormOption.SPECIFY
  );

  const specifyEventDurations: Ref<EventDurationsForm> = ref({
    durations: initialEventDurations.value.durations,
    isValid: initialEventDurations.value.isValid
  });

  const startEndEventDurations: Ref<EventDurationsForm> = ref({
    durations: [],
    isValid: false
  });

  const announceLaterEventDuration: EventDurationsForm = {
    durations: [],
    isValid: true
  };

  const isSpecifyOption = computed(() => {
    return currentOption.value === EventDateTimeFormOption.SPECIFY;
  });

  const isStartEndOption = computed(() => {
    return currentOption.value === EventDateTimeFormOption.START_END;
  });

  const isLaterOption = computed(() => {
    return currentOption.value === EventDateTimeFormOption.LATER;
  });

  function changeOption(option: EventDateTimeFormOption) {
    currentOption.value = option;
  }

  watch(
    () => specifyEventDurations,
    () => {
      if (isSpecifyOption.value)
        context.emit(UPDATE_MODEL_VALUE, specifyEventDurations.value);
    },
    { deep: true }
  );

  watch(
    () => startEndEventDurations,
    () => {
      if (isStartEndOption.value)
        context.emit(UPDATE_MODEL_VALUE, startEndEventDurations.value);
    },
    { deep: true }
  );

  watch(currentOption, () => {
    if (currentOption.value === EventDateTimeFormOption.SPECIFY)
      context.emit(UPDATE_MODEL_VALUE, specifyEventDurations.value);
    else if (currentOption.value === EventDateTimeFormOption.START_END)
      context.emit(UPDATE_MODEL_VALUE, startEndEventDurations.value);
    else context.emit(UPDATE_MODEL_VALUE, announceLaterEventDuration);
  });

  return {
    isSpecifyOption,
    isStartEndOption,
    isLaterOption,
    changeOption,
    specifyEventDurations,
    startEndEventDurations
  };
}