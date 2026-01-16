package me.srinathsilla.tinder_ai_backend.matches;

import me.srinathsilla.tinder_ai_backend.profiles.Profile;

public record Match(String id, Profile profile, String conversationId) {
}