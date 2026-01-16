import { z } from "zod";
import { zodResolver } from "@hookform/resolvers/zod";

const requiredString = (message: string) =>
  z
    .string({
      required_error: message,
      invalid_type_error: message,
    })
    .nonempty(message);

const formSchema = z.object({
  title: requiredString("Введите название"),
  price: z.number().gt(0, "Введите сумму"),
  weight: z.number().gt(0, "Введите вес"),
  measurement: requiredString("Выберите единицу измерения"),
});

export type TFormValues = z.infer<typeof formSchema>;

export const resolver = zodResolver(formSchema);