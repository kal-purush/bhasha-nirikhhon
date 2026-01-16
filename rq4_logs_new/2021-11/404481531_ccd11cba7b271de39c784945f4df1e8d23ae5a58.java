package com.roommatematcher.enterprise.dao;

import com.roommatematcher.enterprise.dto.Profile;

import java.io.IOException;
import java.util.List;

public interface IProfileDAO {
List<Profile> fetchProfile(String filterQuery) throws IOException;
}