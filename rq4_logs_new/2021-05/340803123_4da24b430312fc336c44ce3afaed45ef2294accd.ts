import { computed, reactive, Ref, ref, watch, SetupContext } from "vue";
import { validatePhone, isNumber } from "@/commons/utils/validForm";
import { set } from "date-fns";
import { UPDATE_MODEL_VALUE } from "@/commons/constant";
import { Tag } from "@/apollo/types";
import { EventInfoForm } from "@/commons/Interfaces";
import { testTags } from "../testData";

export default function useEventInfoForm(
  initialEventInfoValue: Ref<EventInfoForm>,
  context: SetupContext<"update:modelValue"[]>
) {
  const name = ref(initialEventInfoValue.value.name);
  const contact = ref(initialEventInfoValue.value.contact);
  const tags: Ref<Tag[]> = ref(initialEventInfoValue.value.tags);
  const tagSearch: Ref<Tag> = ref({ id: -1, name: "", events: [] });
  const description = ref(initialEventInfoValue.value.description);
  const attendeeLimit = ref(
    initialEventInfoValue.value.attendeeLimit.toString()
  );
  const registrationDueDate = ref(
    initialEventInfoValue.value.registrationDueDate
  );
  const registrationDueTime = ref(0);
  const posterImg = ref(initialEventInfoValue.value.posterImg);
  const coverImg = ref(initialEventInfoValue.value.coverImg);

  //Get from API
  const tagOptionValues: Tag[] = testTags;
  const tagOptionNames: string[] = tagOptionValues.map(tag => tag.name);

  const isValidContact = computed(() => {
    return validatePhone(contact.value);
  });

  const isValidAttendeeLimit = computed(() => {
    return isNumber(attendeeLimit.value);
  });

  const attendeeLimitNumber = computed(() => {
    if (!isValidAttendeeLimit.value || attendeeLimit.value === "") return 0;
    return parseInt(attendeeLimit.value);
  });

  const registrationDueDateTime = computed(() => {
    const dateTime = set(new Date(registrationDueDate.value), {
      hours: registrationDueTime.value
    });
    return dateTime.toString();
  });

  const isValidEventInfo = computed(() => {
    if (
      name.value === "" ||
      description.value === "" ||
      attendeeLimitNumber.value === 0
    )
      return false;
    return true;
  });

  function addTag(tag: Tag) {
    const tagId = tag.id;
    const isInTagsList = tags.value.some(
      selectedTag => selectedTag.id === tagId
    );
    if (isInTagsList) return;
    tags.value.push(tagSearch.value);
  }

  function removeTag(index: number) {
    tags.value.splice(index, 1);
  }

  watch(tagSearch, () => {
    if (tagSearch.value.id === -1) return;
    addTag(tagSearch.value);
    tagSearch.value = { id: -1, name: "", events: [] };
  });

  const eventInfo: EventInfoForm = reactive({
    name: name,
    contact: contact,
    tags: tags,
    description: description,
    attendeeLimit: attendeeLimitNumber,
    registrationDueDate: registrationDueDateTime,
    posterImg: posterImg,
    coverImg: coverImg,
    isValid: isValidEventInfo
  });

  watch(
    () => eventInfo,
    () => context.emit(UPDATE_MODEL_VALUE, eventInfo),
    { deep: true }
  );

  return {
    name,
    contact,
    tags,
    tagSearch,
    tagOptionNames,
    tagOptionValues,
    description,
    attendeeLimit,
    registrationDueDate,
    registrationDueTime,
    isValidContact,
    isValidAttendeeLimit,
    removeTag,
    posterImg,
    coverImg
  };
}