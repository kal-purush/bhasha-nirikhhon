import tinymce from "tinymce/tinymce";
export const textEditorInit = tinymce.init({
  menubar: false,
  language: "vi",
  language_url: "/langs/vi.js",
  directionality: "ltr",
  plugins: [
    "advlist",
    "autolink",
    "lists",
    "link",
    "image",
    "charmap",
    "preview",
    "anchor",
    "searchreplace",
    "visualblocks",
    "fullscreen",
    "insertdatetime",
  ],
  toolbar:
    "undo redo | blocks | " +
    "bold italic forecolor | alignleft aligncenter " +
    "alignright alignjustify | bullist numlist outdent indent",
  content_style:
    "body { font-family:Helvetica,Arial,sans-serif; font-size:16px }",
});