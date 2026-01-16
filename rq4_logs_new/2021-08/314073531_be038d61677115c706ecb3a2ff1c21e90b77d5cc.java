/*
 * Copyright (c) 2016-2019 LabKey Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.labkey.test.components.response;

import org.labkey.test.components.BodyWebPart;
import org.labkey.test.components.ext4.Message;
import org.openqa.selenium.WebDriver;

public class MyStudiesResponseServerWebPart extends BodyWebPart<MyStudiesResponseServerWebPart.ElementCache>
{
    public MyStudiesResponseServerWebPart(WebDriver driver)
    {
        super(driver, "MyStudies Response Server");
    }

    @Override
    protected ElementCache newElementCache()
    {
        return new ElementCache();
    }

    public class ElementCache extends BodyWebPart.ElementCache
    {
    }

    public static class ResponseCollectionDialog extends Message
    {
        public static final String WARNING_TITLE = "Response collection stopped";

        public ResponseCollectionDialog(WebDriver wd)
        {
            super(WARNING_TITLE, wd);
        }

        public void clickCancel()
        {
            clickButton("Cancel", true);
        }

        public void clickOk()
        {
            clickButton("OK", true);
        }
    }
}