"use server";

import { deleteFormById } from "@/app/(form)/form/action";
import { nextGetServerSession } from "@/lib/next-auth";
import { FormWithFields } from "@/types/entityRelations";
import prisma from "@/lib/prisma";
import { findForm, findFormWithSubmission } from "@/utils/database/form.query";
import { Prisma } from "@prisma/client";
import generateRandomSlug from "@/utils/randomSlug";

export const deleteForm = async (form_id: string) => {
  try {
    await deleteFormById(form_id);
    return { error: true, message: "Sukses menghapus formulir" };
  } catch (e) {
    console.error(e);
    return { error: true, message: "Gagal menghapus formulir" };
  }
};

export const saveForm = async (data: FormWithFields, is_new = false) => {
  try {
    const session = await nextGetServerSession();
    const { user } = session!;

    if (!is_new) {
      const form = await findFormWithSubmission({ id: data.id });

      if (user?.role !== "SuperAdmin" && user?.id != form?.user_id) {
        return { error: false, message: "Forbidden access" };
      }

      const deletedField = form?.fields.filter(
        (item) => data.fields.findIndex((field) => item.id == field.id) == -1,
      );

      await prisma.field.deleteMany({
        where: { OR: deletedField?.map((item) => ({ id: item.id })) },
      });

      // reject if the form has respondents
      if (form?.submissions.length) {
        return {
          error: true,
          message: "Hapus data responden untuk mengedit formulir",
        };
      }

      await prisma.form.update({
        where: { id: data.id },
        data: { ...data, fields: undefined },
      });

      await Promise.all(
        data.fields.map(async (field, index) => {
          if (field.options.length) {
            await prisma.field_Option.deleteMany({
              where: { field_id: field.id },
            });
          }
          let options = field.options.map((option) => {
            return { ...option, id: undefined };
          });

          let newField = {
            ...field,
            fieldNumber: index + 1,
            form_id: data.id,
            id: undefined,
            options: { createMany: { data: options } },
          };

          await prisma.field.upsert({
            where: { id: field.id == 0 ? undefined : field.id },
            update: newField,
            create: newField,
          });

          return newField;
        }),
      );

      return {
        error: false,
        message: "Berhasil menyimpan formulir",
      };
    } else {
      const form = await prisma.form.create({
        data: { ...data, fields: undefined, id: generateRandomSlug() },
      });

      let fieldsQuery: Prisma.FieldUncheckedCreateInput[] = data.fields.map(
        (field, index) => {
          let options = field.options.map((option) => {
            return { ...option, id: undefined };
          });

          let newField = {
            ...field,
            fieldNumber: index + 1,
            form_id: form.id,
            id: undefined,
            options: { create: options },
          };
          return newField;
        },
      );
      await prisma.field.createMany({ data: fieldsQuery });

      return {
        error: false,
        message: "Berhasil menyimpan formulir",
        data: { id: form.id },
      };
    }
  } catch (e) {
    console.error(e);
    return { error: true, message: "Gagal menyimpan formulir" };
  }
};

export const cloneForm = async (id: string) => {
  try {
    const session = await nextGetServerSession();
    const { user } = session!;

    let form = await findForm({
      id: id,
    });
    if (!form) throw new Error("Form tidak dimukan");

    if (user?.role !== "SuperAdmin" && user?.id != form?.user_id) {
      return { error: false, message: "Forbidden access" };
    }

    const newForm: Prisma.FormUncheckedCreateInput = {
      ...form,
      id: generateRandomSlug(),
      title: "Salinan " + form.title,
      created_at: new Date(),
      fields: {
        createMany: {
          data: form.fields.map((field) => {
            return {
              ...field,
              form_id: undefined,
              id: undefined,
              options: undefined,
            };
          }),
        },
      },
    };

    const createForm = await prisma.form.create({ data: newForm });
    if (!createForm) throw new Error("Terjadi kesalahan saat menyalin");

    const clonedForm = await findForm({ id: createForm.id });

    const options = form.fields
      .map((item, index) => {
        let newOptions = item.options.map((option) => ({
          ...option,
          field_id: clonedForm?.fields[index].id || 0,
          id: undefined,
        }));
        return newOptions;
      })
      .filter((item) => item.length > 0);

    await prisma.field_Option.createMany({ data: options.flat(1) });

    return {
      error: false,
      message: "Berhasil membuat salian",
      data: { id: createForm.id },
    };
  } catch (e) {
    console.error(e);
    return { error: true, message: "Gagal membuat salinan" };
  }
};

export const deleteSubmission = async (id: string) => {
  try {
    await prisma.submission.deleteMany({ where: { id } });
  } catch (e) {
    console.error(e);
    return { error: true, message: "Gagal menghapus formulir" };
  }
};