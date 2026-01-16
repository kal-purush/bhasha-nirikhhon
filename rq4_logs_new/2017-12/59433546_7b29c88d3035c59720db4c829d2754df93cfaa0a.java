
package com.bernardomg.tabletop.dreadball.web.toolkit.test.integration.builder.service;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import com.bernardomg.tabletop.dreadball.build.dbx.model.SponsorAffinities;
import com.bernardomg.tabletop.dreadball.build.dbx.service.SponsorBuilderService;
import com.google.common.collect.Iterables;

@ContextConfiguration(
        locations = { "classpath:context/test-service-context.xml" })
public class ITDefaultSponsorBuilderServiceSelectAffs
        extends AbstractJUnit4SpringContextTests {

    @Autowired
    private SponsorBuilderService service;

    public ITDefaultSponsorBuilderServiceSelectAffs() {
        super();
    }

    @Test
    public final void
            testGetAffinityOptionGroups_AffinitiesAndRank_AddsAdditionalOption() {
        final Collection<String> affinities;
        final SponsorAffinities result;

        affinities = new ArrayList<>();
        affinities.add("affinity_1");
        affinities.add("affinity_2");
        affinities.add("affinity_3");
        affinities.add("affinity_4");
        affinities.add("rank_increase");
        result = service.selectAffinities(affinities);

        Assert.assertEquals(6, result.getBaseRank().intValue());
        Assert.assertEquals(6, result.getRank().intValue());
        Assert.assertEquals(4, Iterables.size(result.getAffinities()));
    }

    @Test
    public final void testGetAffinityOptionGroups_NoAffinities_EmptyResult() {
        final Collection<String> affinities;
        final SponsorAffinities result;

        affinities = new ArrayList<>();
        result = service.selectAffinities(affinities);

        Assert.assertEquals(5, result.getBaseRank().intValue());
        Assert.assertEquals(5, result.getRank().intValue());
        Assert.assertEquals(0, Iterables.size(result.getAffinities()));
    }

}