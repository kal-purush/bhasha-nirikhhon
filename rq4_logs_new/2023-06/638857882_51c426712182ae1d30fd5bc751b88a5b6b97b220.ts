import { computed, ref, watch } from 'vue'
import { useForm, UseFormOptions, SubmitPost } from '../useForm'
import { ElDialogProps } from '../FormDialog'

export type UseFormDialogOptions<T> = UseFormOptions<T> & {
  post: SubmitPost<T>
  onClose?: VoidFunction
  onOpen?: VoidFunction
}

export function useFormDialog<T extends object>(opts: UseFormDialogOptions<T>) {
  const { formState, ...other } = useForm<T>(opts)
  const visible = ref(false)
  const dialogProps = ref({})

  const submit = (post?: SubmitPost<T>) => {
    other.submit(async (model) => {
      post && (await post(model))

      hide()
    })
  }

  const formDialogState = computed(() => {
    return {
      formState: formState.value,
      visible: visible.value,
      submitting: formState.value.submitting,
      'onUpdate:modelValue'(val: boolean) {
        visible.value = val
      },
      onSubmit: () => submit(opts.post),
      ...dialogProps.value,
    }
  })

  const show = (props?: Partial<ElDialogProps> & { onSubmit?: VoidFunction }) => {
    props && (dialogProps.value = props)
    visible.value = true
  }

  const hide = () => {
    visible.value = false
  }

  watch(visible, (val) => {
    val ? opts.onOpen?.() : opts.onClose?.()
  })

  return {
    ...other,
    formDialogState,
    show,
    hide,
    submit,
  }
}