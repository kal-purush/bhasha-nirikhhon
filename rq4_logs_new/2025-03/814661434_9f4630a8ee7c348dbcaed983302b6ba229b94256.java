/*
 * DT-Tool
 * Copyright (c) 2024-present Carsten Rambow
 * mailto:developer AT elomagic DOT de
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
package de.elomagic.dttool.commands;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import picocli.CommandLine;

import de.elomagic.dttool.ComparatorFactory;
import de.elomagic.dttool.ConsoleOptions;
import de.elomagic.dttool.ConsolePrinter;
import de.elomagic.dttool.DTrackClient;
import de.elomagic.dttool.ProjectFilterOptions;
import de.elomagic.dttool.model.Project;

import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.util.List;

public class AbstractProjectFilterCommand {

    private static final ConsolePrinter LOGGER = ConsolePrinter.INSTANCE;

    @CommandLine.Mixin
    DTrackClient client;
    @CommandLine.Mixin
    ProjectFilterOptions projectFilterOptions;
    @CommandLine.Mixin
    ConsoleOptions consoleOptions;

    @Nonnull
    protected List<Project> fetchProjects(@Nullable String versionMatchRegEx) {

        ZonedDateTime notBefore = ZonedDateTime.now().minusDays(projectFilterOptions.getOlderThenDays());

        LOGGER.info("Version match: {}", versionMatchRegEx);
        LOGGER.info("Fetching projects with name/uid {}", projectFilterOptions.getProjectFilter());
        LOGGER.info("Fetching projects which not older then {} days", projectFilterOptions.getOlderThenDays());

        List<Project> projects = client
                .fetchAllProjects()
                .stream()
                .sorted(ComparatorFactory.nameComparator())
                .sorted(ComparatorFactory.versionComparator())
                .filter(p -> projectFilterOptions.getProjectFilter().isEmpty() || projectFilterOptions.getProjectFilter().contains(p.getName()))
                .filter(p -> p.getLastBomImport() == null || notBefore.isAfter(p.getLastBomImport()))
                .filter(p -> StringUtils.isBlank(versionMatchRegEx) || p.getVersion().matches(versionMatchRegEx))
                .toList();

        projects.forEach(p -> LOGGER.info("{}\t {}\t Created {}", p.getName(), p.getVersion(), p.getLastBomImport()));
        LOGGER.info("{} projects matched ", projects.size());

        return projects;

    }

}