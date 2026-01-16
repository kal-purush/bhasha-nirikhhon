package com.evolveum.polygon.hcm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.filter.AndFilter;
import org.identityconnectors.framework.common.objects.filter.AttributeFilter;
import org.identityconnectors.framework.common.objects.filter.CompositeFilter;
import org.identityconnectors.framework.common.objects.filter.ContainsAllValuesFilter;
import org.identityconnectors.framework.common.objects.filter.ContainsFilter;
import org.identityconnectors.framework.common.objects.filter.EndsWithFilter;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.framework.common.objects.filter.FilterVisitor;
import org.identityconnectors.framework.common.objects.filter.GreaterThanFilter;
import org.identityconnectors.framework.common.objects.filter.GreaterThanOrEqualFilter;
import org.identityconnectors.framework.common.objects.filter.LessThanFilter;
import org.identityconnectors.framework.common.objects.filter.LessThanOrEqualFilter;
import org.identityconnectors.framework.common.objects.filter.NotFilter;
import org.identityconnectors.framework.common.objects.filter.OrFilter;
import org.identityconnectors.framework.common.objects.filter.StartsWithFilter;

public class FilterHandler implements FilterVisitor<Boolean, String> {

	private static final Log LOGGER = Log.getLog(FilterHandler.class);

	private static final String UID = "__UID__";
	private static final String DELIMITER = "\\.";

	private static final String EQUALS = "eq";
	private static final String CONTAINS = "co";
	private static final String STARTSWITH = "sw";
	private static final String ENDSWITH = "ew";
	// private static final String GREATERTHAN = "gt";
	// private static final String GREATEROREQ = "ge";
	// private static final String LESSTHAN = "lt";
	// private static final String LESSOREQ = "le";
	private static final String AND = "and";
	private static final String OR = "or";
	private static final String NOT = "not";

	@Override
	public Boolean visitAndFilter(String p, AndFilter filter) {

		Boolean outcome = evaluateCompositeFilter(filter, AND, p);
		return outcome;
	}

	@Override
	public Boolean visitContainsFilter(String p, ContainsFilter filter) {
		Boolean outcome = evaluateAttributeFilter(filter, CONTAINS, p);
		return outcome;
	}

	@Override
	public Boolean visitContainsAllValuesFilter(String p, ContainsAllValuesFilter filter) {
		// TODO
		Collection<Filter> filterList = buildValueList(filter);
		AndFilter containFilters = (AndFilter) FilterBuilder.and(filterList);
		Boolean outcome = containFilters.accept(this, p);

		return outcome;
	}

	@Override
	public Boolean visitEqualsFilter(String p, EqualsFilter filter) {

		Boolean outcome = evaluateAttributeFilter(filter, EQUALS, p);
		return outcome;
	}

	@Override
	public Boolean visitExtendedFilter(String p, Filter filter) {
		LOGGER.warn("Usuported filter: {0}", filter);
		return false;
	}

	@Override
	public Boolean visitGreaterThanFilter(String p, GreaterThanFilter filter) {
		LOGGER.warn("Usuported filter: {0}", filter);
		return false;
	}

	@Override
	public Boolean visitGreaterThanOrEqualFilter(String p, GreaterThanOrEqualFilter filter) {
		LOGGER.warn("Usuported filter: {0}", filter);
		return false;
	}

	@Override
	public Boolean visitLessThanFilter(String p, LessThanFilter filter) {
		LOGGER.warn("Usuported filter: {0}", filter);
		return false;
	}

	@Override
	public Boolean visitLessThanOrEqualFilter(String p, LessThanOrEqualFilter filter) {
		LOGGER.warn("Usuported filter: {0}", filter);
		return false;
	}

	@Override
	public Boolean visitNotFilter(String p, NotFilter filter) {

		Boolean outcome = filter.getFilter().accept(this, p);
		return !outcome;

	}

	@Override
	public Boolean visitOrFilter(String p, OrFilter filter) {
		// TODO
		LOGGER.warn("Usuported filter: {0}", filter);
		return false;

	}

	@Override
	public Boolean visitStartsWithFilter(String p, StartsWithFilter filter) {
		Boolean outcome = evaluateAttributeFilter(filter, STARTSWITH, p);
		return outcome;
	}

	@Override
	public Boolean visitEndsWithFilter(String p, EndsWithFilter filter) {
		Boolean outcome = evaluateAttributeFilter(filter, ENDSWITH, p);
		return outcome;
	}

	private Boolean evaluateAttributeFilter(AttributeFilter filter, String filterType, String p) {

		if (p == null || p.isEmpty()) {
			return true;
		} else {

			String endTagName = "";
			String evaluatedValue = "";
			String uidAttributeName = "";

			String heplerVariableParts[] = p.split(DELIMITER);

			if (heplerVariableParts.length != 3) {
				return true;
			} else {
				endTagName = heplerVariableParts[0];
				evaluatedValue = heplerVariableParts[1];
				uidAttributeName = heplerVariableParts[2];
			}

			Attribute attribute = filter.getAttribute();
			String attributeValue = AttributeUtil.getAsStringValue(attribute);
			String attributeName = attribute.getName();

			if ((attributeName.equals(UID) && uidAttributeName.equals(endTagName))
					|| attributeName.equals(endTagName)) {

				if (EQUALS.equals(filterType)) {
					if (!attributeValue.equals(evaluatedValue)) {
						return false;
					}
				} else if (CONTAINS.equals(filterType)) {
					if (!StringUtils.containsIgnoreCase(evaluatedValue, attributeValue)) {

						return false;
					}

				} else if (STARTSWITH.equals(filterType)) {
					if (!StringUtils.startsWithIgnoreCase(evaluatedValue, attributeValue)) {

						return false;
					}

				} else if (ENDSWITH.equals(filterType)) {
					if (!StringUtils.endsWithIgnoreCase(evaluatedValue, attributeValue)) {

						return false;
					}

				}
			}
		}
		return true;

	}

	private Boolean evaluateCompositeFilter(CompositeFilter filter, String filterType, String p) {

		Boolean outcome = true;

		for (Filter processedFilter : filter.getFilters()) {

			outcome = processedFilter.accept(this, p);

			if (AND.equals(filterType)) {
				if (!outcome) {
					return outcome;
				}
			}
		}
		return outcome;

	}

	private Collection<Filter> buildValueList(ContainsAllValuesFilter filter) {

		List<Object> valueList = filter.getAttribute().getValue();
		Collection<Filter> filterList = new ArrayList<Filter>();

		for (Object value : valueList) {

			Filter containsSingleAtribute = (ContainsFilter) FilterBuilder
					.contains(AttributeBuilder.build(filter.getName(), value));
			filterList.add(containsSingleAtribute);
		}

		return filterList;
	}

}