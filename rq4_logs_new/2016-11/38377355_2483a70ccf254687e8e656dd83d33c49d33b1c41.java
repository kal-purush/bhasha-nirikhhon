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

import com.google.gson.Gson;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import net.sourceforge.stripes.controller.DispatcherServlet;
import net.sourceforge.stripes.controller.StripesFilter;
import net.sourceforge.stripes.controller.json.GsonJsonContentTypeRequestWrapper;
import net.sourceforge.stripes.mock.MockRoundtrip;
import net.sourceforge.stripes.mock.MockServletContext;
import net.sourceforge.stripes.util.Log;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static net.sourceforge.stripes.controller.DefaultContentTypeRequestWrapperFactory.CONTENT_TYPE_REQUEST_WRAPPER_CONFIGS;

/**
 *
 * @author Néstor Hernández Loli
 */
public class PluggableContentTypeRequestWrapperTest {

    private static final Log LOG = Log.getInstance(GsonRestActionBeanTest.class);

    private MockServletContext gsonServletContext;
    private MockServletContext jacksonServletContext;

    @BeforeClass
    public void initCtx() {
        gsonServletContext = createServletContextGson();
        jacksonServletContext = createServletContextJackson();
    }

    @AfterClass
    public void closeCtx() {
        gsonServletContext.close();
        jacksonServletContext.close();
    }

    @Test(groups = "fast")
    public void testJsonParsingWithGson() throws Exception {
        executeTestParsingBody(gsonServletContext, "application/json");
    }

    @Test(groups = "fast")
    public void testJsonParsingWithJackson() throws Exception {
        executeTestParsingBody(jacksonServletContext, "application/json");
    }
   

    private void executeTestParsingBody(MockServletContext servletContext, String contentType) throws Exception {
        MockRoundtrip trip = new MockRoundtrip(servletContext, PersonActionBean.class);
        trip.getRequest().addHeader("content-type", contentType);
        trip.getRequest().setMethod("GET");

        PersonActionBean personActionBean = new PersonActionBean();
        Person person = new Person();
        person.setAge(10);
        person.setName("Blaba");
        personActionBean.setPerson(person);

        Gson gson = new Gson();
        String jsonStr = gson.toJson(personActionBean);
        trip.getRequest().setRequestBody(jsonStr);

        trip.execute();
        Assert.assertEquals(trip.getResponse().getStatus(), HttpURLConnection.HTTP_OK);

        PersonActionBean personActionBeanResult = trip.getActionBean(PersonActionBean.class);
        Person personResult = personActionBeanResult.getPerson();

        Assert.assertEquals(personResult.getName(), person.getName());
        Assert.assertEquals(personResult.getAge(), person.getAge());
    }
 
    private MockServletContext createServletContextGson() {
        Map<String, String> filterParams = new HashMap<String, String>();
        filterParams.put("ActionResolver.Packages", "net.sourceforge.stripes");
        filterParams.put("LocalePicker.Class", "net.sourceforge.stripes.localization.MockLocalePicker");
        filterParams.put(CONTENT_TYPE_REQUEST_WRAPPER_CONFIGS, "application/json=" + GsonJsonContentTypeRequestWrapper.class.getName());

        return new MockServletContext("testPCT1")
                .addFilter(StripesFilter.class, "StripesFilterPCT1", filterParams)
                .setServlet(DispatcherServlet.class, "StripesDispatcherPCT1", null);
    }

    private MockServletContext createServletContextJackson() {
        Map<String, String> filterParams = new HashMap<String, String>();
        filterParams.put("ActionResolver.Packages", "net.sourceforge.stripes");
        filterParams.put("LocalePicker.Class", "net.sourceforge.stripes.localization.MockLocalePicker");

        //Huh! None here, default is Jackson!
        //filterParams.put(CONTENT_TYPE_REQUEST_WRAPPER_CLASSES, "application/json=" + GsonJsonContentTypeRequestWrapper.class.getName());
        return new MockServletContext("testPCT2")
                .addFilter(StripesFilter.class, "StripesFilterPCT2", filterParams)
                .setServlet(DispatcherServlet.class, "StripesDispatcherPCT2", null);
    }

    private void logTripResponse(MockRoundtrip trip) {
        LOG.debug("TRIP RESPONSE: [Status=" + trip.getResponse().getStatus()
                + "] [Message=" + trip.getResponse().getOutputString() + "] [Error Message="
                + trip.getResponse().getErrorMessage() + "]");
    }

    @RestActionBean
    @UrlBinding("/person")
    public static class PersonActionBean implements ActionBean {

        private ActionBeanContext context;
        private Person person;

        @GET
        public Resolution get() {
            int i = 0;
            return new StreamingResolution("text/plain", "Hello world!");
        }

        public Person getPerson() {
            return person;
        }

        public void setPerson(Person person) {
            this.person = person;
        }

        public ActionBeanContext getContext() {
            return context;
        }

        public void setContext(ActionBeanContext context) {
            this.context = context;
        }
    }

    public static class Person {

        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

    }

}