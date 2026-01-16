/**
 * Copyright 2016 the original author or authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.bernardomg.tabletop.dreadball.web.toolkit.test.unit.controller.builder;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.bernardomg.tabletop.dreadball.build.dbx.model.DefaultSponsorAffinities;
import com.bernardomg.tabletop.dreadball.build.dbx.model.SponsorAffinities;
import com.bernardomg.tabletop.dreadball.build.dbx.service.SponsorBuilderService;
import com.bernardomg.tabletop.dreadball.web.toolkit.controller.builder.dbx.SponsorAssemblerController;
import com.bernardomg.tabletop.dreadball.web.toolkit.controller.codex.AffinityGroupCodexController;
import com.bernardomg.tabletop.dreadball.web.toolkit.test.configuration.UrlDbxTeamBuilderConfig;
import com.google.common.collect.Iterables;

/**
 * Unit tests for {@link AffinityGroupCodexController}, checking the results of
 * REST requests.
 * 
 * @author Bernardo Mart&iacute;nez Garrido
 */
public final class TestSponsorAssemblerControllerAffinities {

    private ArgumentCaptor<Iterable> captor;

    /**
     * Mocked MVC context.
     */
    private MockMvc                  mockMvc;

    private SponsorBuilderService    service;

    /**
     * Default constructor;
     */
    public TestSponsorAssemblerControllerAffinities() {
        super();
    }

    /**
     * Sets up the mocked MVC context.
     */
    @Before
    public final void setUpMockContext() {
        service = getSponsorBuilderService();
        mockMvc = MockMvcBuilders.standaloneSetup(getController(service))
                .alwaysExpect(MockMvcResultMatchers.status().isOk()).build();
    }

    @Test
    public final void testGet_AffParam_Affinities() throws Exception {
        mockMvc.perform(getGetRequest("affinity_1"));

        Assert.assertEquals(1, Iterables.size(captor.getValue()));
    }

    @Test
    public final void testGet_NoParams_NoAffinities() throws Exception {
        mockMvc.perform(getGetRequest());

        Assert.assertEquals(0, Iterables.size(captor.getValue()));
    }

    /**
     * Returns a mocked controller.
     * <p>
     * It has all the dependencies stubbed.
     * 
     * @return a mocked controller
     */
    private final SponsorAssemblerController
            getController(final SponsorBuilderService sponsorBuilderService) {
        return new SponsorAssemblerController(sponsorBuilderService);
    }

    /**
     * Returns a request builder for getting the tested form view.
     * 
     * @return a request builder for the form view
     */
    private final RequestBuilder getGetRequest() {
        return MockMvcRequestBuilders
                .get(UrlDbxTeamBuilderConfig.URL_VALIDATE_AFFINITIES);
    }

    private final RequestBuilder getGetRequest(final String affinity) {
        return MockMvcRequestBuilders
                .get(UrlDbxTeamBuilderConfig.URL_VALIDATE_AFFINITIES
                        + "?affinities={}", affinity);
    }

    private final SponsorBuilderService getSponsorBuilderService() {
        final SponsorBuilderService sponsorBuilderService;
        final SponsorAffinities result;

        sponsorBuilderService = Mockito.mock(SponsorBuilderService.class);

        result = new DefaultSponsorAffinities(Collections.emptyList(), 0);

        captor = ArgumentCaptor.forClass(Iterable.class);
        Mockito.when(sponsorBuilderService.selectAffinities(captor.capture()))
                .thenReturn(result);

        return sponsorBuilderService;
    }

}