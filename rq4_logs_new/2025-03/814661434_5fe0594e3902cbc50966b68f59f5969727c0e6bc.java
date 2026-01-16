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

import picocli.CommandLine;

import de.elomagic.dttool.DtToolException;

import org.cyclonedx.Version;
import org.cyclonedx.exception.GeneratorException;
import org.cyclonedx.generators.BomGeneratorFactory;
import org.cyclonedx.model.Bom;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@CommandLine.Command(name = "collects-boms", description = "Collect BOMs of projects")
public class CollectBomsCommand extends AbstractProjectFilterCommand implements Callable<Void> {

    @CommandLine.Option(
            names = { "-fp", "--filePattern" },
            description = "Target file pattern, %1 -> Filename, %2 -> Version",
            required = true,
            defaultValue = "%1-%2.bom.json"
    )
    private String pattern;
    @CommandLine.Option(
            names = { "-f", "--file" },
            description = "Target file or path",
            required = true
    )
    private Path path;

    @Override
    public Void call() throws Exception {

        List<Bom> boms = fetchProjects(null)
                .stream()
                .map(p -> client.fetchProjectBom(p.getUuid()))
                .toList();

        Files.createDirectories(path.getParent());

        boolean compressed = path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".zip");

        if (compressed) {
            writeIntoZIP(boms, path);
        } else {
            writeIntoFolder(boms, path);
        }

        return null;
    }

    private void writeIntoFolder(List<Bom> boms, Path target) {
        boms.forEach(bom -> {
            try {
                Path targetFile = target.resolve(createFilename(bom));

                Files.writeString(targetFile, getBomAsJson(bom));
            } catch (Exception ex) {
                throw new DtToolException(ex);
            }
        });
    }

    private void writeIntoZIP(List<Bom> boms, Path target) throws Exception {

        try (FileOutputStream fos = new FileOutputStream(target.toFile());
             ZipOutputStream zipOut = new ZipOutputStream(fos)) {

            for (Bom bom : boms) {
                ZipEntry zipEntry = new ZipEntry(createFilename(bom));
                zipOut.putNextEntry(zipEntry);
                zipOut.write(getBomAsJson(bom).getBytes(StandardCharsets.UTF_8));
            }
        }

    }

    private String getBomAsJson(Bom bom) throws GeneratorException {
        Version version = Arrays
                .stream(Version.values())
                .filter(v -> v.getVersionString().equals(bom.getSpecVersion()))
                .findFirst()
                .orElseThrow(() -> new DtToolException("Unsupported version: " + bom.getSpecVersion()));

        return BomGeneratorFactory.createJson(version, bom).toJsonString();
    }

    private String createFilename(Bom bom) {
        return pattern.formatted(
                bom.getMetadata().getComponent().getName(),
                bom.getMetadata().getComponent().getVersion()
        );
    }
}