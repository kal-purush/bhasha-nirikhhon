package com.github.ssullivan.db;

import com.github.ssullivan.model.Facility;
import com.github.ssullivan.model.FacilityWithRadius;
import com.github.ssullivan.model.Page;
import com.github.ssullivan.model.SearchRequest;
import com.github.ssullivan.model.SearchResults;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface IFacilitySearchDao {

  /**
   * Finds the all of the facilities that any of the specified service codes withhin a certain
   * radius.
   *
   * @param serviceCodes the list of service codes facilities can have
   * @param longitude the longitude coordinate according to WGS84
   * @param latitude the latitude coordinate according to WGS84
   * @param distance radius distance
   * @param geoUnit distance unit (m, km, ft, mi)
   */
  SearchResults<FacilityWithRadius> findByServiceCodesWithin(final List<String> serviceCodes,
      double longitude, double latitude, double distance, final String geoUnit, Page page)
      throws IOException;

  /**
   * Finds the all of the facilities that any of the specified service codes withhin a certain
   * radius.
   *
   * @param mustServiceCodes the list of service codes facilities can have
   * @param mustNotServiceCodes the list of service codes facilities can't have
   * @param longitude the longitude coordinate according to WGS84
   * @param latitude the latitude coordinate according to WGS84
   * @param distance radius distance
   * @param geoUnit distance unit (m, km, ft, mi)
   */
  SearchResults<FacilityWithRadius> findByServiceCodesWithin(
      final Collection<String> mustServiceCodes,
      final Collection<String> mustNotServiceCodes,
      final boolean matchAny,
      final double longitude,
      final double latitude,
      final double distance,
      final String geoUnit,
      final Page page) throws IOException;

  default SearchResults<FacilityWithRadius> findByServiceCodesWithin(
      final Collection<String> mustServiceCodes,
      final Collection<String> mustNotServiceCodes,
      final double longitude,
      final double latitude,
      final double distance,
      final String geoUnit,
      final Page page) throws IOException {

    return findByServiceCodesWithin(mustServiceCodes, mustNotServiceCodes, false,
        longitude, latitude, distance, geoUnit, page);
  }

  CompletionStage<SearchResults<Facility>> find(final SearchRequest searchRequest,
      final Page page) throws Exception;

  /**
   * Finds the all of the facilities that any of the specified service codes.
   *
   * @param serviceCodes the SAMSHA service codes
   * @param page control how many results to return
   */
  SearchResults<Facility> findByServiceCodes(final Collection<String> serviceCodes, final Page page)
      throws IOException;

  SearchResults<Facility> findByServiceCodes(final Collection<String> serviceCodes,
      final Collection<String> mustNotServiceCodes,
      final boolean matchAny, final Page page)
      throws IOException;

  SearchResults findByServiceCodes(final ImmutableSet<String> mustServiceCodes, final Page page)
      throws IOException;
}