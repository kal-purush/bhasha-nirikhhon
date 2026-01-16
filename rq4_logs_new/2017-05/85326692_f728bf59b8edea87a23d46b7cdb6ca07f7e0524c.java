package com.arrow.acn.client.api;

import java.net.URI;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import com.arrow.acn.client.AcnClientException;
import com.arrow.acn.client.model.SoftwareReleaseScheduleModel;
import com.arrow.acn.client.model.SoftwareReleaseScheduleRegistrationModel;
import com.arrow.acn.client.search.SoftwareReleaseScheduleSearchCriteria;
import com.arrow.acs.JsonUtils;
import com.arrow.acs.client.api.ApiConfig;
import com.arrow.acs.client.model.HidModel;
import com.arrow.acs.client.model.PagingResultModel;
import com.fasterxml.jackson.core.type.TypeReference;

public class SoftwareReleaseScheduleApi extends ApiAbstract {

	private static final String SOFTWARE_RELEASE_SCHEDULE_BASE_URL = API_BASE + "/software/releases/schedules";
	private static final String CREATE_URL = SOFTWARE_RELEASE_SCHEDULE_BASE_URL;
	private static final String SPECIFIC_SOFTWARE_RELEASE_URL = SOFTWARE_RELEASE_SCHEDULE_BASE_URL + "/%s";
	private static final String UPDATE_URL = SPECIFIC_SOFTWARE_RELEASE_URL;
	private static final String FIND_BY_HID = SPECIFIC_SOFTWARE_RELEASE_URL;
	private static final String FIND_ALL_BY_URL = SOFTWARE_RELEASE_SCHEDULE_BASE_URL;

	private static final TypeReference<PagingResultModel<SoftwareReleaseScheduleModel>> SOFTWARE_RELEASE_SCHEDULE_MODEL_TYPE_REF = new TypeReference<PagingResultModel<SoftwareReleaseScheduleModel>>() {
	};

	SoftwareReleaseScheduleApi(ApiConfig apiConfig) {
		super(apiConfig);
	}

	public HidModel create(SoftwareReleaseScheduleRegistrationModel model) {
		String method = "create";
		try {
			URI uri = buildUri(CREATE_URL);
			HidModel result = execute(new HttpPost(uri), JsonUtils.toJson(model), HidModel.class);
			log(method, result);
			return result;
		} catch (Throwable e) {
			logError(method, e);
			throw new AcnClientException(method, e);
		}
	}

	public HidModel update(String hid, SoftwareReleaseScheduleRegistrationModel model) {
		String method = "update";
		try {
			URI uri = buildUri(String.format(UPDATE_URL, hid));
			HidModel result = execute(new HttpPut(uri), JsonUtils.toJson(model), HidModel.class);
			log(method, result);
			return result;
		} catch (Throwable e) {
			logError(method, e);
			throw new AcnClientException(method, e);
		}
	}

	public SoftwareReleaseScheduleModel findByHid(String hid) {
		String method = "findByHid";
		try {
			URI uri = buildUri(String.format(FIND_BY_HID, hid));
			SoftwareReleaseScheduleModel result = execute(new HttpGet(uri), SoftwareReleaseScheduleModel.class);
			log(method, result);
			return result;
		} catch (Throwable e) {
			logError(method, e);
			throw new AcnClientException(method, e);
		}
	}

	public PagingResultModel<SoftwareReleaseScheduleModel> findAllBy(SoftwareReleaseScheduleSearchCriteria criteria) {
		String method = "findAllBy";
		try {
			URI uri = buildUri(FIND_ALL_BY_URL, criteria);
			PagingResultModel<SoftwareReleaseScheduleModel> result = execute(new HttpGet(uri),
			        SOFTWARE_RELEASE_SCHEDULE_MODEL_TYPE_REF);
			log(method, result);
			return result;
		} catch (Throwable e) {
			logError(method, e);
			throw new AcnClientException(method, e);
		}
	}
}