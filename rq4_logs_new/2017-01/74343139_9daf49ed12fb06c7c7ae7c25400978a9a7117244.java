package com.ft.methodeimagemodelmapper.resources;

import com.ft.api.jaxrs.errors.ClientError;
import com.ft.api.jaxrs.errors.ServerError;
import com.ft.api.util.transactionid.TransactionIdUtils;
import com.ft.content.model.Content;
import com.ft.methodeimagemodelmapper.exception.ContentMapperException;
import com.ft.methodeimagemodelmapper.exception.MethodeContentNotSupportedException;
import com.ft.methodeimagemodelmapper.exception.TransformationException;
import com.ft.methodeimagemodelmapper.exception.ValidationException;
import com.ft.methodeimagemodelmapper.messaging.MessageProducingContentMapper;
import com.ft.methodeimagemodelmapper.model.EomFile;
import com.ft.methodeimagemodelmapper.service.MethodeImageModelMapper;
import com.ft.methodeimagemodelmapper.validation.PublishingValidator;
import com.ft.methodeimagemodelmapper.validation.UuidValidator;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.Date;

@Path("/")
public class MethodeImageModelResource {

    private static final String CHARSET_UTF_8 = ";charset=utf-8";

    private static final String CONTENT_TYPE_NOT_SUPPORTED = "Unsupported type - not an image.";
    private static final String CONTENT_CANNOT_BE_MAPPED = "Content cannot be mapped.";
    private static final String INVALID_UUID = "Invalid uuid";
    private static final String UNABLE_TO_WRITE_JSON_MESSAGE = "Unable to write JSON for message";

    private final MethodeImageModelMapper methodeImageModelMapper;
    private final MessageProducingContentMapper messageProducingContentMapper;
    private final UuidValidator uuidValidator;
    private final PublishingValidator publishingValidator;


    public MethodeImageModelResource(MethodeImageModelMapper methodeImageModelMapper,
                                     MessageProducingContentMapper messageProducingContentMapper,
                                     UuidValidator uuidValidator,
                                     PublishingValidator publishingValidator) {
        this.methodeImageModelMapper = methodeImageModelMapper;
        this.messageProducingContentMapper = messageProducingContentMapper;
        this.uuidValidator = uuidValidator;
        this.publishingValidator = publishingValidator;
    }

    @POST
    @Path("/map")
    @Produces(MediaType.APPLICATION_JSON + CHARSET_UTF_8)
    public final Content mapImageModel(EomFile methodeContent, @Context HttpHeaders httpHeaders) {
        return getModelAndHandleExceptions(httpHeaders, (transactionId) ->
                methodeImageModelMapper.mapImageModel(methodeContent, transactionId, null));
    }

    @POST
    @Path("/ingest")
    public final void ingestImageModel(EomFile methodeContent, @Context HttpHeaders httpHeaders) {
        getModelAndHandleExceptions(httpHeaders, (transactionId) -> {
            Content content = null;
            uuidValidator.validate(methodeContent.getUuid());
            if (publishingValidator.isValidForPublishing(methodeContent)) {
                content = messageProducingContentMapper.mapImageModel(methodeContent, transactionId, new Date());
            }
            return content;
        });
    }

    private Content getModelAndHandleExceptions(HttpHeaders headers, Action<Content> getContentModel) {
        final String transactionId = TransactionIdUtils.getTransactionIdOrDie(headers);
        try {
            return getContentModel.perform(transactionId);
        } catch (IllegalArgumentException | ValidationException e) {
            throw ClientError.status(400).error(INVALID_UUID).exception(e);
        } catch (MethodeContentNotSupportedException e) {
            throw ClientError.status(400).error(CONTENT_TYPE_NOT_SUPPORTED).exception(e);
        } catch (TransformationException e) {
            throw ClientError.status(400).error(CONTENT_CANNOT_BE_MAPPED).exception(e);
        } catch (ContentMapperException e) {
            throw ServerError.status(500).error(UNABLE_TO_WRITE_JSON_MESSAGE).exception(e);
        }
    }

    private interface Action<T> {
        T perform(String transactionId);
    }
}