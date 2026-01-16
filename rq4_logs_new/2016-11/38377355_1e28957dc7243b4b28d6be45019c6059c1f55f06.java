/*
 * Copyright 2016 Stripes Framework.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sourceforge.stripes.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Néstor Hernández Loli
 */
public class HttpResolution implements Resolution {

    private int status;

    public HttpResolution(int status) {
        this.status = status;
    }

    @Override
    public void execute(HttpServletRequest request, HttpServletResponse response) throws Exception {
        response.setStatus(status);
    }

    public int getStatus() {
        return status;
    }

    public HttpResolution setStatus(int status) {
        this.status = status;
        return this;
    }

}