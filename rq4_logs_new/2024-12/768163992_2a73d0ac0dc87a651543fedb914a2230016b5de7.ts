import { CommentTag, DeclarationReflection, Reflection } from "typedoc"

export const getTagsAsArray = (tag: CommentTag): string[] => {
  return tag.content
    .map((content) => content.text)
    .join("")
    .split(",")
    .map((value) => value.trim())
}

export const getTagComments = (reflection: Reflection): CommentTag[] => {
  const tagComments: CommentTag[] = []

  reflection.comment?.blockTags
    .filter((tag) => tag.tag === `@tags`)
    .forEach((tag) => tagComments.push(tag))

  if (reflection instanceof DeclarationReflection) {
    reflection.signatures?.forEach((signature) =>
      tagComments.push(...getTagComments(signature))
    )
  }

  return tagComments
}