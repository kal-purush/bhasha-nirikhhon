import { stringLiteral } from "@execonline-inc/decoders";
import {
  linksDecoder as linksDecoderR,
  resourceDecoder as resourceDecoderR,
  ResourceWithErrors,
  resourceWithErrorsDecoder as resourceWithErrorsDecoderR,
  ResourceWithMetadata,
  resourceWithMetadataDecoder as resourceWithMetadataDecoderR,
} from "@execonline-inc/resource";
import Decoder, {
  array,
  boolean,
  field,
  maybe,
  string,
  succeed,
} from "jsonous";
import { Maybe } from "maybeasy";
import {
  Embedded,
  Rel,
  Resource,
  ResourceWithActions,
  ResourceWithEmbedded,
  toRel,
  ValidationError,
  ValidationErrors,
} from "../Types";

export const linksDecoder = linksDecoderR(toRel);

export const resourceDecoder: <T>(
  payloadDecoder: Decoder<T>
) => Decoder<Resource<T>> = resourceDecoderR(toRel);

export const resourceWithErrorsDecoder: <T>(
  payloadDecoder: Decoder<T>
) => Decoder<ResourceWithErrors<T, Rel>> = resourceWithErrorsDecoderR(toRel);

export const resourceWithMetadataDecoder: <T, M>(
  payloadDecoder: Decoder<T>,
  metadataDecoder: Decoder<M>
) => Decoder<ResourceWithMetadata<T, M, Rel>> =
  resourceWithMetadataDecoderR(toRel);

export const resourceWithActionsDecoder = <T, A>(
  payloadDecoder: Decoder<T>,
  actionsDecoder: Decoder<A>
): Decoder<ResourceWithActions<T, A>> =>
  succeed({})
    .assign("links", field("links", linksDecoder))
    .assign("payload", field("data", payloadDecoder))
    .assign("actions", field("actions", actionsDecoder));

export const embeddedResourceDecoder = <T>(
  rel: Rel,
  decoder: Decoder<T>
): Decoder<Maybe<Resource<T>>> => maybe(field(rel, resourceDecoder(decoder)));

export const resourceWithEmbeddedDecoder = <T, E>(
  payloadDecoder: Decoder<T>,
  embeddedDecoder: Decoder<Embedded<E>>
): Decoder<ResourceWithEmbedded<T, E>> =>
  resourceDecoder(payloadDecoder).assign(
    "embedded",
    field("embedded", embeddedDecoder)
  );

export { errorDecoder } from "@execonline-inc/resource";

export const validationErrorDecoder: Decoder<ValidationError> = succeed({})
  .assign("kind", field("kind", stringLiteral("validation-error")))
  .assign("title", field("title", stringLiteral("Invalid Attribute")))
  .assign("resource", field("resource", string))
  .assign("attributePath", field("attribute_path", string))
  .assign("attribute", field("attribute", string))
  .assign("detail", field("detail", string))
  .assign("error", field("error", string));

export const validationErrorsDecoder: Decoder<ValidationErrors> = array(
  validationErrorDecoder
);

export const optionalBooleanDecoder = (fieldName: string) =>
  maybe(field(fieldName, boolean)).map((id) => id.getOrElseValue(false));