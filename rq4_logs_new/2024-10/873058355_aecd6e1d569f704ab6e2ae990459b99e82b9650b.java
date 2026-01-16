/**
 * SPDX-FileCopyrightText: (c) 2024 Liferay, Inc. https://liferay.com
 * SPDX-License-Identifier: LGPL-2.1-or-later OR LicenseRef-Liferay-DXP-EULA-2.0.0-2023-06
 */
package com.liferay.workshop.holiday.sb.exception;

import com.liferay.portal.kernel.exception.NoSuchModelException;

/**
 * @author Brian Wing Shun Chan
 */
public class NoSuchHolidayException extends NoSuchModelException {

	public NoSuchHolidayException() {
	}

	public NoSuchHolidayException(String msg) {
		super(msg);
	}

	public NoSuchHolidayException(String msg, Throwable throwable) {
		super(msg, throwable);
	}

	public NoSuchHolidayException(Throwable throwable) {
		super(throwable);
	}

}