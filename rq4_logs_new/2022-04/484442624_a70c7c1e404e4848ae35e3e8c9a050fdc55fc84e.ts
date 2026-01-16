import S from "@sanity/desk-tool/structure-builder";
import { MdChatBubble, MdAccountCircle } from "react-icons/md";
import resolveProductionUrl from "./resolveProductionUrl";
import Iframe from "sanity-plugin-iframe-pane";
// We filter document types defined in structure to prevent
// them from being listed twice
const hiddenDocTypes = (listItem) => !["post", "author", "media.tag"].includes(listItem.getId());

export const getDefaultDocumentNode = () => {
    // Return all documents with just 1 view: the form
    return S.document().views([
        S.view.form(),
        S.view
            .component(Iframe)
            .options({
                // Accepts an async function
                url: (doc) => resolveProductionUrl(doc),
            })
            .title("Preview"),
    ]);
};

export default () =>
    S.list()
        .title("Site")
        .items([
            S.listItem()
                .title("Posts")
                .icon(MdChatBubble)
                .schemaType("post")
                .child(S.documentTypeList("post").title("Posts")),
            S.listItem()
                .title("Authors")
                .icon(MdAccountCircle)
                .schemaType("author")
                .child(S.documentTypeList("author").title("Authors")),
            ...S.documentTypeListItems().filter(hiddenDocTypes),
        ]);