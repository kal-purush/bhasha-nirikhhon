/*
 * WorldGuard, a suite of tools for Minecraft
 * Copyright (C) sk89q <http://www.sk89q.com>
 * Copyright (C) WorldGuard team and contributors
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.sk89q.worldguard.util.profile.resolver;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.sk89q.worldguard.util.profile.Profile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public class NoLowerCombinedProfileService extends CombinedProfileService {
    private final List<ProfileService> services;

    public NoLowerCombinedProfileService(List<ProfileService> services) {
        Preconditions.checkNotNull(services);
        this.services = ImmutableList.copyOf(services);
    }

    @Override
    public Profile findByName(String name) throws IOException, InterruptedException {
        for (ProfileService service : services) {
            Profile profile = service.findByName(name);
            if (profile != null) {
                return profile;
            }
        }
        return null;
    }

    @Override
    public ImmutableList<Profile> findAllByName(Iterable<String> names) throws IOException, InterruptedException {
        List<String> missing = new ArrayList<>();
        List<Profile> totalResults = new ArrayList<>();

        for (String name : names) {
            missing.add(name);
        }

        for (ProfileService service : services) {
            ImmutableList<Profile> results = service.findAllByName(missing);

            for (Profile profile : results) {
                missing.removeIf(name -> name.equalsIgnoreCase(profile.getName()));
                totalResults.add(profile);
            }

            if (missing.isEmpty()) {
                break;
            }
        }

        return ImmutableList.copyOf(totalResults);
    }

    @Override
    public void findAllByName(Iterable<String> names, final Predicate<Profile> consumer) throws IOException, InterruptedException {
        final List<String> missing = Collections.synchronizedList(new ArrayList<>());

        Predicate<Profile> forwardingConsumer = profile -> {
            missing.removeIf(name -> name.equalsIgnoreCase(profile.getName()));
            return consumer.test(profile);
        };

        for (String name : names) {
            missing.add(name);
        }

        for (ProfileService service : services) {
            service.findAllByName(new ArrayList<>(missing), forwardingConsumer);

            if (missing.isEmpty()) {
                break;
            }
        }
    }
}