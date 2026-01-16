/*
  Copyright 2023 Adobe. All rights reserved.
  This file is licensed to you under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License. You may obtain a copy
  of the License at http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software distributed under
  the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
  OF ANY KIND, either express or implied. See the License for the specific language
  governing permissions and limitations under the License.
*/

package com.adobe.marketing.mobile.edge.consent;

import static com.adobe.marketing.mobile.edge.consent.ConsentConstants.LOG_TAG;

import com.adobe.marketing.mobile.services.Log;
import com.adobe.marketing.mobile.util.CloneFailedException;
import com.adobe.marketing.mobile.util.EventDataUtils;
import java.util.Map;

final class Utils {

	private static final String LOG_SOURCE = "Utils";

	private Utils() {}

	/**
	 * Creates a deep copy of the provided {@link Map}.
	 *
	 * @param map to be copied
	 * @return {@link Map} containing a deep copy of all the elements in {@code map}
	 */
	static Map<String, Object> deepCopy(final Map<String, Object> map) {
		try {
			return EventDataUtils.clone(map);
		} catch (CloneFailedException e) {
			Log.debug(
				LOG_TAG,
				LOG_SOURCE,
				"Unable to deep copy map. CloneFailedException: %s",
				e.getLocalizedMessage()
			);
		}

		return null;
	}
}