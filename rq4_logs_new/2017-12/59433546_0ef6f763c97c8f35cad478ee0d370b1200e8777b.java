
package com.bernardomg.tabletop.dreadball.web.toolkit.test.unit.builder.assembler;

import java.util.Collections;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.bernardomg.tabletop.dreadball.build.dbx.model.OptionGroup;
import com.bernardomg.tabletop.dreadball.build.dbx.service.DefaultSponsorBuilderService;
import com.bernardomg.tabletop.dreadball.build.dbx.service.SponsorBuilderAssemblerService;
import com.bernardomg.tabletop.dreadball.build.dbx.service.SponsorBuilderService;
import com.bernardomg.tabletop.dreadball.service.model.SponsorAffinityGroupAvailabilityService;
import com.bernardomg.tabletop.dreadball.service.model.SponsorUnitsService;
import com.google.common.collect.Iterables;

public class TestDefaultSponsorBuilderServiceAffOptions {

    public TestDefaultSponsorBuilderServiceAffOptions() {
        super();
    }

    @Test
    public final void testGetAffinityOptionGroups_NoAffinities_Empty() {
        final SponsorBuilderService service;
        final Iterable<OptionGroup> options;

        service = getNoAffinitiesSponsorBuilderService();

        options = service.getAffinityOptionGroups();

        Assert.assertEquals(0, Iterables.size(options));
    }

    private final SponsorBuilderService getNoAffinitiesSponsorBuilderService() {
        final SponsorBuilderAssemblerService sponsorBuilderAssemblerService;
        final SponsorUnitsService sponsorUnitsService;
        final SponsorAffinityGroupAvailabilityService sponsorAffinityGroupAvailabilityService;

        sponsorBuilderAssemblerService = Mockito
                .mock(SponsorBuilderAssemblerService.class);
        sponsorUnitsService = Mockito.mock(SponsorUnitsService.class);

        sponsorAffinityGroupAvailabilityService = Mockito
                .mock(SponsorAffinityGroupAvailabilityService.class);
        Mockito.when(sponsorAffinityGroupAvailabilityService
                .getAllSponsorAffinityGroupAvailabilities())
                .thenReturn(Collections.emptyList());

        return new DefaultSponsorBuilderService(sponsorBuilderAssemblerService,
                sponsorUnitsService, sponsorAffinityGroupAvailabilityService);
    }

}