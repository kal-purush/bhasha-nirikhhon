/*
 * Copyright 2020 PC Chin. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pcchin.studyassistant;

import android.content.Context;

import androidx.test.platform.app.InstrumentationRegistry;

import com.pcchin.studyassistant.functions.SecurityFunctions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test hashing/encryption functions that require context. **/
public class ContextSecurityTest {
    private static final int TEST_COUNT = 10000;
    private Context instrumentationContext;

    @Before
    public void setup() {
        instrumentationContext = InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

    /** Test whether the RSA encryption is working. **/
    @Test
    public void testRSA() {
        String original = AndroidTestFunctions.randomString(TEST_COUNT);
        String after = SecurityFunctions.RSAServerEncrypt(instrumentationContext, original);
        Assert.assertNotNull(after);
        String after2 = SecurityFunctions.RSAServerEncrypt(instrumentationContext, original);
        Assert.assertNotNull(after2);
        Assert.assertEquals(after, after2);
    }
}