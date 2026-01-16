package org.richardinnocent.feefo.api.v10.reviews.nps;

import com.fasterxml.jackson.core.type.TypeReference;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.stream.Collectors;
import org.richardinnocent.feefo.api.requests.EqualityOperator;
import org.richardinnocent.feefo.api.requests.FeefoApiGetRequest;
import org.richardinnocent.feefo.api.requests.QueryParameter;
import org.richardinnocent.feefo.api.v10.reviews.nps.NpsReviewsRequest.Builder.FinalStageBuilder;
import org.richardinnocent.feefo.api.v10.reviews.nps.params.SortField;
import org.richardinnocent.feefo.api.v10.reviews.shared.Comparison;
import org.richardinnocent.feefo.api.v10.reviews.shared.params.Inclusion;
import org.richardinnocent.feefo.api.v10.reviews.shared.params.MediaInclusion;
import org.richardinnocent.feefo.api.v10.reviews.shared.params.TagFilter;
import org.richardinnocent.feefo.api.v10.reviews.shared.params.TimeFrame;
import org.richardinnocent.feefo.api.v10.reviews.shared.params.UnansweredFeedbackInclusion;

/**
 * Retrieves customer feedback details with NPS content. The response will contain
 * personally-identifiable or business-sensitive information, so authorisation not required.
 */
public class NpsReviewsRequest extends FeefoApiGetRequest<NpsReviewsResponse> {

  private static final TypeReference<NpsReviewsResponse> RESPONSE_TYPE =
      new TypeReference<NpsReviewsResponse>() {};

  private final Collection<QueryParameter> queryParameters = new ArrayList<>();

  private NpsReviewsRequest(FinalStageBuilder builder) {
    super(RESPONSE_TYPE);
    buildQueryParameters(builder);
  }

  private void buildQueryParameters(FinalStageBuilder builder) {
    if (builder.sortField != null) {
      queryParameters.add(new QueryParameter("sort", builder.sortField.getQueryKey()));
    }

    if (builder.page != null) {
      queryParameters.add(new QueryParameter("page", builder.page.toString()));
    }

    if (builder.pageSize != null) {
      queryParameters.add(new QueryParameter("page_size", builder.pageSize.toString()));
    }

    if (!builder.responseFields.isEmpty()) {
      queryParameters.add(new QueryParameter("fields", String.join(",", builder.responseFields)));
    }

    queryParameters.add(new QueryParameter("merchant_identifier", builder.merchantIdentifier));

    if (!builder.tagFilters.isEmpty()) {
      queryParameters.add(
          new QueryParameter(
              "tags",
              builder.tagFilters.stream()
                                .map(TagFilter::toQueryValue)
                                .collect(Collectors.joining(","))
          )
      );
    }

    builder.reviewCreationTimeComparisons
        .stream()
        .map(
            comparison ->
                new QueryParameter(
                    "date_time", comparison.getEqualityOperator(), comparison.getReference().toString()
                )
        )
        .forEach(queryParameters::add);

    builder.reviewUpdatedTimeComparisons
        .stream()
        .map(
            comparison ->
                new QueryParameter(
                    "updated_date_time",
                    comparison.getEqualityOperator(),
                    comparison.getReference().toString()
                )
        )
        .forEach(queryParameters::add);

    if (builder.reviewCreatedSinceComparison != null) {
      queryParameters.add(
          new QueryParameter("since_period", builder.reviewCreatedSinceComparison.getReference())
      );
    }

    if (builder.reviewUpdatedSinceComparison != null) {
      queryParameters.add(
          new QueryParameter(
              "since_updated_period", builder.reviewUpdatedSinceComparison.getReference()
          )
      );
    }

    if (builder.requestOrigin != null) {
      queryParameters.add(new QueryParameter("origin", builder.requestOrigin));
    }

    if (builder.feedbackId != null) {
      queryParameters.add(new QueryParameter("id", builder.feedbackId));
    }

    if (builder.parentProductSku != null) {
      queryParameters.add(new QueryParameter("parent_product_sku", builder.parentProductSku));
    }

    if (builder.productSku != null) {
      queryParameters.add(new QueryParameter("product_sku", builder.productSku));
    }

    if (builder.customerReference != null) {
      queryParameters.add(new QueryParameter("customer_reference", builder.customerReference));
    }

    if (builder.customerEmail != null) {
      queryParameters.add(new QueryParameter("customer_email", builder.customerEmail));
    }

    if (builder.orderReference != null) {
      queryParameters.add(new QueryParameter("order_reference", builder.orderReference));
    }

    builder.feedbackScores
        .stream()
        .map(Object::toString)
        .map(score -> new QueryParameter("rating", score))
        .forEach(queryParameters::add);

    if (builder.feedbackFromSubMerchantsInclusion != null) {
      queryParameters.add(
          new QueryParameter("children", builder.feedbackFromSubMerchantsInclusion.getQueryValue())
      );
    }

    if (builder.mediaInclusion != null) {
      queryParameters.add(
          new QueryParameter("media", builder.mediaInclusion.getQueryValue())
      );
    }

    if (builder.unansweredFeedbackInclusion != null) {
      queryParameters.add(
          new QueryParameter(
              "unanswered_feedback", builder.unansweredFeedbackInclusion.getQueryValue()
          )
      );
    }

    if (builder.fullThreadInclusion != null) {
      queryParameters.add(
          new QueryParameter("full_thread", builder.fullThreadInclusion.getQueryValue())
      );
    }

    if (builder.enhancedInsightInclusion != null) {
      queryParameters.add(
          new QueryParameter("enhanced_insight", builder.enhancedInsightInclusion.getQueryValue())
      );
    }
  }

  @Override
  protected String getBasePath() {
    return "/10/reviews/nps";
  }

  @Override
  protected Collection<QueryParameter> getQueryParameters() {
    return queryParameters;
  }

  @Override
  public boolean requiresAuthentication() {
    return true;
  }

  /**
   * Creates a new builder for creating {@link NpsReviewsRequest} objects.
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link NpsReviewsRequest} objects.
   */
  public static class Builder {

    private Builder() {}

    /**
     * Specifies the merchant to retrieve reviews for. Note that sub-merchants can also by included
     * in the request by specifying
     * {@link FinalStageBuilder#withFeedbackFromSubMerchants(Inclusion)}.
     * @param merchantIdentifier The identifier for the merchant.
     * @return A builder.
     * @throws NullPointerException Thrown if {@code merchantIdentifier == null}.
     */
    public FinalStageBuilder forMerchant(String merchantIdentifier) throws NullPointerException {
      return new FinalStageBuilder(merchantIdentifier);
    }

    /**
     * Builder for {@link NpsReviewsRequest} objects.
     */
    public static class FinalStageBuilder {
      private SortField sortField;
      private Integer page;
      private Integer pageSize;
      private final Collection<String> responseFields = new ArrayList<>();

      private final String merchantIdentifier;
      private final Collection<TagFilter> tagFilters = new ArrayList<>();
      private final Collection<Comparison<OffsetDateTime>> reviewCreationTimeComparisons =
          new ArrayList<>(2);
      private final Collection<Comparison<OffsetDateTime>> reviewUpdatedTimeComparisons =
          new ArrayList<>(2);
      private Comparison<String> reviewCreatedSinceComparison;
      private Comparison<String> reviewUpdatedSinceComparison;
      private String requestOrigin;
      private String feedbackId;
      private String parentProductSku;
      private String productSku;
      private String customerReference;
      private String customerEmail;
      private String orderReference;
      private final Collection<Integer> feedbackScores = new HashSet<>(5);
      private Inclusion feedbackFromSubMerchantsInclusion;
      private MediaInclusion mediaInclusion;
      private UnansweredFeedbackInclusion unansweredFeedbackInclusion;
      private Inclusion fullThreadInclusion;
      private Inclusion enhancedInsightInclusion;

      private FinalStageBuilder(String merchantIdentifier)
          throws NullPointerException, IllegalArgumentException {
        this.merchantIdentifier =
            Objects.requireNonNull(
                merchantIdentifier, "Merchant identifier is mandatory and cannot be null");
        if (merchantIdentifier.isEmpty()) {
          throw new IllegalArgumentException(
              "Merchant identifier is mandatory and cannot be empty");
        }
      }

      /**
       * Specifies the primary field by which the response should be sorted.
       * @param sortField The primary field by which the response should be sorted.
       * @return This builder.
       */
      public FinalStageBuilder withSortField(SortField sortField) {
        this.sortField = sortField;
        return this;
      }

      /**
       * Specifies the page of results that should be returned from this request.
       * @param page The page of results to retrieve.
       * @return This builder.
       * @throws IllegalArgumentException Thrown if {@code page < 1}.
       */
      public FinalStageBuilder withPage(int page) throws IllegalArgumentException {
        if (page < 1) {
          throw new IllegalArgumentException("Page must be positive but is " + page);
        }
        this.page = page;
        return this;
      }

      /**
       * Specifies the maximum number of sales that will be returned from this request.
       * @param pageSize The maximum number of sales that will be returned from this request. Must
       * be between 1 and 100 (inclusive).
       * @return This builder.
       * @throws IllegalArgumentException Thrown if {@code pageSize < 1 || pageSize > 100}.
       */
      public FinalStageBuilder withPageSize(int pageSize) throws IllegalArgumentException {
        if (pageSize < 1 || pageSize > 100) {
          throw new IllegalArgumentException(
              "Page size must be between 1 and 100 (inclusive), but is " + pageSize
          );
        }
        this.pageSize = pageSize;
        return this;
      }

      /**
       * The API can be restricted such that it only returns a subset of response fields. This
       * method will ensure that only this provided field is returned. Note that this method is
       * cumulative, so calling {@code .withResponseField("a").withResponseField("b")} is
       * functionally identical to {@code .withResponseFields("a", "b")}.
       * @param responseField The field that should be returned.
       * @return This builder.
       * @throws NullPointerException Thrown if {@code responseField == null}.
       */
      public FinalStageBuilder withResponseField(String responseField) throws NullPointerException {
        this.responseFields.add(Objects.requireNonNull(responseField, "Response field is null"));
        return this;
      }

      /**
       * The API can be restricted such that it only returns a subset of response fields. This
       * method will ensure that only these provided fields are returned. Note that this method is
       * cumulative, so calling {@code .withResponseFields("a", "b").withResponseFields("c", "d")}
       * is functionally identical to {@code .withResponseFields("a", "b", "c", "d")}.
       * @param responseField1 The first response field.
       * @param responseFields The remaining response fields.
       * @return This builder.
       * @throws NullPointerException Thrown if any of the response fields are {@code null}.
       */
      public FinalStageBuilder withResponseFields(String responseField1, String... responseFields)
          throws NullPointerException {
        withResponseField(responseField1);
        if (responseFields != null) {
          for (String responseField : responseFields) {
            withResponseField(responseField);
          }
        }
        return this;
      }

      /**
       * The API can be restricted such that it only returns a subset of response fields. This
       * method will ensure that only these provided fields are returned. Note that this method is
       * cumulative, so calling
       * {@code .withResponseFields(Arrays.asList("a", "b")).withResponseFields(Arrays.asList("c", "d"))}
       * is functionally identical to {@code .withResponseFields("a", "b", "c", "d")}.
       * @param responseFields The fields that should be returned.
       * @return This builder.
       * @throws NullPointerException Thrown if any of the response fields are {@code null}.
       */
      public FinalStageBuilder withResponseFields(Collection<String> responseFields)
          throws NullPointerException {
        if (responseFields != null) {
          responseFields.forEach(this::withResponseField);
        }
        return this;
      }

      /**
       * Sales can be filtered by the tags that they contain. This method adds the specified tag
       * filter to the request.
       * @param tagFilter The tag filter to add.
       * @return This builder.
       * @throws NullPointerException Thrown if {@code tagFilter == null}.
       */
      public FinalStageBuilder withTagFilter(TagFilter tagFilter) throws NullPointerException {
        tagFilters.add(Objects.requireNonNull(tagFilter, "Tag filter is null"));
        return this;
      }

      /**
       * Sales can be filtered by the tags that they contain. This method adds all of the specified
       * tag filters to the request.
       * @param tagFilter1 The first tag filter to add.
       * @param tagFilters The remaining tag filters to add.
       * @return This builder.
       * @throws NullPointerException Thrown if any tag filter is {@code null}.
       */
      public FinalStageBuilder withTagFilters(TagFilter tagFilter1, TagFilter... tagFilters)
          throws NullPointerException {
        withTagFilter(tagFilter1);
        if (tagFilters != null) {
          for (TagFilter filter : tagFilters) {
            withTagFilter(filter);
          }
        }
        return this;
      }

      /**
       * Sales can be filtered by the tags that they contain. This method adds all of the specified
       * tag filters to the request.
       * @param tagFilters The remaining tag filters to add.
       * @return This builder.
       * @throws NullPointerException Thrown if any tag filter is {@code null}.
       */
      public FinalStageBuilder withTagFilters(Collection<TagFilter> tagFilters)
          throws NullPointerException {
        if (tagFilters != null) {
          tagFilters.forEach(this::withTagFilter);
        }
        return this;
      }

      /**
       * Specifies that only reviews that were created exactly at the specified time should be
       * returned.
       * @param reviewCreationTime The time that the review was created.
       * @return This builder.
       * @throws NullPointerException Thrown if {@code reviewCreationTime == null}.
       */
      public FinalStageBuilder withReviewCreationTimeAtExactly(OffsetDateTime reviewCreationTime)
          throws NullPointerException {
        reviewCreationTimeComparisons.add(
            new Comparison<>(EqualityOperator.EQUALS, reviewCreationTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were created before the specified time should be returned.
       * @param latestReviewCreationTime The latest time that a review could have been created
       * (exclusive).
       * @return This builder.
       * @throws NullPointerException Thrown if {@code latestReviewCreationTime == null}.
       */
      public FinalStageBuilder withReviewCreationTimeBefore(
          OffsetDateTime latestReviewCreationTime) throws NullPointerException {
        reviewCreationTimeComparisons.add(
            new Comparison<>(EqualityOperator.LESS_THAN, latestReviewCreationTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were created at or before the specified time should be
       * returned.
       * @param latestReviewCreationTime The latest time that a review could have been created
       * (inclusive).
       * @return This builder.
       * @throws NullPointerException Thrown if {@code latestReviewCreationTime == null}.
       */
      public FinalStageBuilder withReviewCreationTimeAtOrBefore(
          OffsetDateTime latestReviewCreationTime) throws NullPointerException {
        reviewCreationTimeComparisons.add(
            new Comparison<>(EqualityOperator.LESS_THAN_OR_EQUAL_TO, latestReviewCreationTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were created after the specified time should be returned.
       * @param earliestReviewCreationTime The earliest time that a review could have been created
       * (exclusive).
       * @return This builder.
       * @throws NullPointerException Thrown if {@code earliestReviewCreationTime == null}.
       */
      public FinalStageBuilder withReviewCreationTimeAfter(
          OffsetDateTime earliestReviewCreationTime) throws NullPointerException {
        reviewCreationTimeComparisons.add(
            new Comparison<>(EqualityOperator.GREATER_THAN, earliestReviewCreationTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were created at or after the specified time should be
       * returned.
       * @param earliestReviewCreationTime The earliest time that a review could have been created
       * (inclusive).
       * @return This builder.
       * @throws NullPointerException Thrown if {@code earliestReviewCreationTime == null}.
       */
      public FinalStageBuilder withReviewCreationTimeAtOrAfter(
          OffsetDateTime earliestReviewCreationTime) throws NullPointerException {
        reviewCreationTimeComparisons.add(
            new Comparison<>(EqualityOperator.GREATER_THAN_OR_EQUAL_TO, earliestReviewCreationTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were updated exactly at the specified time should be
       * returned.
       * @param reviewUpdatedTime The time that the review was updated.
       * @return This builder.
       * @throws NullPointerException Thrown if {@code reviewUpdatedTime == null}.
       */
      public FinalStageBuilder withReviewUpdatedTimeAtExactly(OffsetDateTime reviewUpdatedTime)
          throws NullPointerException {
        reviewUpdatedTimeComparisons.add(new Comparison<>(EqualityOperator.EQUALS, reviewUpdatedTime));
        return this;
      }

      /**
       * Specifies that only reviews that were updated before the specified time should be returned.
       * @param latestReviewUpdatedTime The latest time that a review could have been updated
       * (exclusive).
       * @return This builder.
       * @throws NullPointerException Thrown if {@code latestReviewUpdatedTime == null}.
       */
      public FinalStageBuilder withReviewUpdatedTimeBefore(OffsetDateTime latestReviewUpdatedTime)
          throws NullPointerException {
        reviewUpdatedTimeComparisons.add(
            new Comparison<>(EqualityOperator.LESS_THAN, latestReviewUpdatedTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were updated at or before the specified time should be
       * returned.
       * @param latestReviewUpdatedTime The latest time that a review could have been updated
       * (inclusive).
       * @return This builder.
       * @throws NullPointerException Thrown if {@code latestReviewUpdatedTime == null}.
       */
      public FinalStageBuilder withReviewUpdatedTimeAtOrBefore(
          OffsetDateTime latestReviewUpdatedTime) throws NullPointerException {
        reviewUpdatedTimeComparisons.add(
            new Comparison<>(EqualityOperator.LESS_THAN_OR_EQUAL_TO, latestReviewUpdatedTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were updated after the specified time should be returned.
       * @param earliestReviewUpdatedTime The earliest time that a review could have been updated
       * (exclusive).
       * @return This builder.
       * @throws NullPointerException Thrown if {@code earliestReviewUpdatedTime == null}.
       */
      public FinalStageBuilder withReviewUpdatedTimeAfter(OffsetDateTime earliestReviewUpdatedTime)
          throws NullPointerException {
        reviewUpdatedTimeComparisons.add(
            new Comparison<>(EqualityOperator.GREATER_THAN, earliestReviewUpdatedTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were updated at or after the specified time should be
       * returned.
       * @param earliestReviewUpdatedTime The earliest time that a review could have been updated
       * (inclusive).
       * @return This builder.
       * @throws NullPointerException Thrown if {@code earliestReviewUpdatedTime == null}.
       */
      public FinalStageBuilder withReviewUpdatedTimeAtOrAfter(
          OffsetDateTime earliestReviewUpdatedTime) throws NullPointerException {
        reviewUpdatedTimeComparisons.add(
            new Comparison<>(EqualityOperator.GREATER_THAN_OR_EQUAL_TO, earliestReviewUpdatedTime)
        );
        return this;
      }

      /**
       * Specifies that only reviews that were created in the given time frame (relative to now)
       * should be returned.
       * @param reviewCreationTimeFrame The time frame.
       * @return This builder.
       * @throws NullPointerException Thrown if {@code reviewCreationTimeFrame == null}.
       */
      public FinalStageBuilder withReviewCreatedIn(TimeFrame reviewCreationTimeFrame)
          throws NullPointerException {
        reviewCreatedSinceComparison =
            new Comparison<>(EqualityOperator.EQUALS, reviewCreationTimeFrame.getQueryValue());
        return this;
      }

      /**
       * Specifies that only reviews that were updated in the given time frame (relative to now)
       * should be returned.
       * @param reviewUpdatedTimeFrame The time frame.
       * @return This builder.
       * @throws NullPointerException Thrown if {@code reviewUpdatedTimeFrame == null}.
       */
      public FinalStageBuilder withReviewUpdatedIn(TimeFrame reviewUpdatedTimeFrame)
          throws NullPointerException {
        reviewUpdatedSinceComparison =
            new Comparison<>(EqualityOperator.EQUALS, reviewUpdatedTimeFrame.getQueryValue());
        return this;
      }

      /**
       * Specifies the domain of the web page that is making the request. This should be used
       * executing cross-origin requests (see
       * <a href="https://support.feefo.com/support/solutions/articles/8000051105-reviews-api-origin">the support docs</a>
       * for more information).
       * @param requestOrigin The domain of the web page that is making the request.
       * @return This builder.
       */
      public FinalStageBuilder withRequestOrigin(String requestOrigin) {
        this.requestOrigin = requestOrigin;
        return this;
      }

      /**
       * Specifies that only feedback with the given ID should be returned.
       * @param feedbackId The ID of the feedback.
       * @return This builder.
       */
      public FinalStageBuilder withFeedbackId(String feedbackId) {
        this.feedbackId = feedbackId;
        return this;
      }

      /**
       * Specifies that only reviews of products with the specified parent product SKU should be
       * returned.
       * @param parentProductSku The parent product SKU of the products of interest.
       * @return This builder.
       */
      public FinalStageBuilder withParentProductSku(String parentProductSku) {
        this.parentProductSku = parentProductSku;
        return this;
      }

      /**
       * Specifies that only reviews of products with the given product SKU should be returned.
       * @param productSku The product SKU of the product of interest.
       * @return This builder.
       */
      public FinalStageBuilder withProductSku(String productSku) {
        this.productSku = productSku;
        return this;
      }

      /**
       * Specifies that only reviews from the customer with the given reference identifier should
       * be returned.
       * @param customerReference The unique customer reference for the customer of interest.
       * @return This builder.
       */
      public FinalStageBuilder withCustomerReference(String customerReference) {
        this.customerReference = customerReference;
        return this;
      }

      /**
       * Specifies that only reviews from the customer with the given email address should be
       * returned.
       * @param email The email address for the customer of interest.
       * @return This builder.
       */
      public FinalStageBuilder withCustomerEmail(String email) {
        this.customerEmail = email;
        return this;
      }

      /**
       * Specifies that only feedback associated with the sale described by the given order
       * reference should be returned.
       * @param orderReference The order reference of the sale of interest.
       * @return This builder.
       */
      public FinalStageBuilder withOrderReference(String orderReference) {
        this.orderReference = orderReference;
        return this;
      }

      /**
       * Specifies that only feedback with the specified score should be returned. This is
       * cumulative, such that {@code .withFeedbackScore(1).withFeedbackScore(2)} is functionally
       * equivalent to {@code .withFeedbackScores(1, 2)}.
       * @param score The feedback score of interest. The score must be between 1 and 5 (inclusive).
       * @return This builder.
       * @throws IllegalArgumentException Thrown if {@code score < 1 || score > 5}.
       */
      public FinalStageBuilder withFeedbackScore(int score) throws IllegalArgumentException {
        if (score < 1 || score > 5) {
          throw new IllegalArgumentException("Score must be between 1 and 5 but is " + score);
        }
        this.feedbackScores.add(score);
        return this;
      }

      /**
       * Specifies that only feedback with the specified scores should be returned. This is
       * cumulative, such that {@code .withFeedbackScore(1, 2).withFeedbackScore(3, 4)} is
       * functionally equivalent to {@code .withFeedbackScores(1, 2, 3, 4)}.
       * @param score1 The first score.
       * @param scores The remaining scores.
       * @return This builder.
       * @throws IllegalArgumentException Thrown if any score is less than 1 or greater than 5.
       */
      public FinalStageBuilder withFeedbackScores(int score1, int... scores)
          throws IllegalArgumentException {
        withFeedbackScore(score1);
        if (scores != null) {
          Arrays.stream(scores).forEach(this::withFeedbackScore);
        }
        return this;
      }

      /**
       * Specifies that only feedback with the specified scores should be returned. This is
       * cumulative, such that
       * {@code .withFeedbackScore(Arrays.asList(1, 2)).withFeedbackScore(Arrays.asList(3, 4))} is
       * functionally equivalent to {@code .withFeedbackScores(1, 2, 3, 4)}.
       * @param scores The scores of interest.
       * @return This builder.
       * @throws IllegalArgumentException Thrown if any score is less than 1 or greater than 5.
       */
      public FinalStageBuilder withFeedbackScores(Collection<Integer> scores)
          throws NullPointerException, IllegalArgumentException {
        if (scores != null) {
          scores.forEach(this::withFeedbackScore);
        }
        return this;
      }

      /**
       * <p>Specifies whether to include feedback from sub-merchants.</p>
       * <p>By default, feedback from sub-merchants is excluded.</p>
       * @param inclusion Whether to include feedback from sub-merchants.
       * @return This builder.
       */
      public FinalStageBuilder withFeedbackFromSubMerchants(Inclusion inclusion) {
        this.feedbackFromSubMerchantsInclusion = inclusion;
        return this;
      }

      /**
       * <p>Specifies how feedback with media should be included in the results.</p>
       * <p>Default: {@link MediaInclusion#INCLUDE}</p>
       * @param mediaInclusion Whether to include feedback with media.
       * @return This builder.
       */
      public FinalStageBuilder withMediaInclusion(MediaInclusion mediaInclusion) {
        this.mediaInclusion = mediaInclusion;
        return this;
      }

      /**
       * <p>Specifies whether feedback left in the last 48 hours but has not yet been responded to
       * by the merchant should be included in the results.</p>
       * <p>Default: {@link UnansweredFeedbackInclusion#INCLUDE}</p>
       * @param unansweredFeedbackInclusion Whether to feedback left in the last 48 hours that has
       * not yet been responded to by the merchant.
       * @return This builder.
       */
      public FinalStageBuilder withUnansweredFeedbackInclusion(
          UnansweredFeedbackInclusion unansweredFeedbackInclusion) {
        this.unansweredFeedbackInclusion = unansweredFeedbackInclusion;
        return this;
      }

      /**
       * <p>Specifies whether subsequent exchanges between the customer and merchant should be
       * included in the results.</p>
       * <p>Default: {@link Inclusion#EXCLUDE}</p>
       * @param fullThreadInclusion Whether subsequent exchanges between the customer and merchant
       * should be included in the results.
       * @return This builder.
       */
      public FinalStageBuilder withFullThread(Inclusion fullThreadInclusion) {
        this.fullThreadInclusion = fullThreadInclusion;
        return this;
      }

      /**
       * <p>Specifies if enhanced insight (smart themes) data should be included in the API
       * response. This is only applicable for merchants with the
       * <a href="https://www.feefo.com/en/business/products/smart-themes">Smart Themes</a> package
       * enabled.</p>
       * <p>Default: {@link Inclusion#EXCLUDE}</p>
       * @param enhancedInsightInclusion Whether to include enhanced insight data in the response.
       * @return This builder.
       */
      public FinalStageBuilder withEnhancedInsight(Inclusion enhancedInsightInclusion) {
        this.enhancedInsightInclusion = enhancedInsightInclusion;
        return this;
      }

      /**
       * Builds the request.
       * @return The request.
       */
      public NpsReviewsRequest build() {
        return new NpsReviewsRequest(this);
      }
    }
  }
}