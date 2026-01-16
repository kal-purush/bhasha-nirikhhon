package com.boozallen.aissemble.upgrade.migration.version_specific;

/*-
 * #%L
 * aiSSEMBLE::Foundation::Upgrade
 * %%
 * Copyright (C) 2021 Booz Allen
 * %%
 * This software package is licensed under the Booz Allen Public License. All Rights Reserved.
 * #L%
 */

import com.boozallen.aissemble.upgrade.migration.AbstractPomMigration;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.technologybrewery.baton.BatonException;
import org.technologybrewery.baton.util.FileUtils;
import org.technologybrewery.baton.util.pom.PomHelper;
import org.technologybrewery.baton.util.pom.PomModifications;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.technologybrewery.baton.util.pom.LocationAwareMavenReader.END;
import static org.technologybrewery.baton.util.pom.LocationAwareMavenReader.START;

/**
 * This migration updates the pom file to remove the data encryption dependencies that are no longer supported.
 */
public class DataEncryptionRemovalPomMigration extends AbstractPomMigration {

    public static final String GROUP_ID= "com.boozallen.aissemble";
    public static final String FOUNDATION_ENCRYPTION = "foundation-encryption-policy-java";
    public static final String EXTENSION_ENCRYPTION = "extensions-encryption-vault-java";

    @Override
    protected boolean shouldExecuteOnFile(File pomFile) {
        try {
            return FileUtils.hasRegExMatch("<artifactId>[\\n\\s]*(foundation\\-encryption\\-policy\\-java|extensions\\-encryption-vault\\-java)[\\n\\s]*<\\/artifactId>", pomFile);
        } catch (IOException e) {
            throw new BatonException("Could not check pom file for data encryption dependencies: " + pomFile.getPath(), e);
        }
    }

    @Override
    protected boolean performMigration(File pomFile) {
        Model model = PomHelper.getLocationAnnotatedModel(pomFile);

        PomModifications modifications = new PomModifications();
        List<Dependency> dataEncryptionDependencies = this.getMatchingDependenciesForProject(model, DataEncryptionRemovalPomMigration::isDataEncryptionDependency);

        for (Dependency dependency : dataEncryptionDependencies) {
            modifications.add(new PomModifications.Deletion(dependency.getLocation(START), dependency.getLocation(END)));
        }
        if (!modifications.isEmpty()) {
            PomHelper.writeModifications(pomFile, modifications.finalizeMods());
        }
        return true;
    }

    private static boolean isDataEncryptionDependency(Dependency dep) {
        return dep.getGroupId().equals(GROUP_ID) &&
                (dep.getArtifactId().equals(FOUNDATION_ENCRYPTION) || dep.getArtifactId().equals(EXTENSION_ENCRYPTION));
    }
}