/*
 * Copyright 2020 PC Chin. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pcchin.studyassistant.functions;

import android.content.Context;

import androidx.annotation.NonNull;
import androidx.room.Room;

import com.pcchin.studyassistant.activity.ActivityConstants;
import com.pcchin.studyassistant.database.notes.NotesSubjectMigration;
import com.pcchin.studyassistant.database.notes.SubjectDatabase;
import com.pcchin.studyassistant.database.project.ProjectDatabase;
import com.pcchin.studyassistant.fragment.project.create.ProjectCreateFragment;
import com.pcchin.studyassistant.utils.misc.RandomString;

/** Database related functions used throughout the app. **/
public final class DatabaseFunctions {
    private DatabaseFunctions() {
        throw new IllegalStateException("Utility class");
    }

    /** Returns the subject database. **/
    @NonNull
    public static SubjectDatabase getSubjectDatabase(Context context) {
        return Room.databaseBuilder(context, SubjectDatabase.class,
                ActivityConstants.DATABASE_NOTES).allowMainThreadQueries()
                .addMigrations(NotesSubjectMigration.MIGRATION_1_2).build();
    }

    /** Returns the project database. **/
    @NonNull
    public static ProjectDatabase getProjectDatabase(Context context) {
        return Room.databaseBuilder(context, ProjectDatabase.class,
                ActivityConstants.DATABASE_PROJECT)
                .fallbackToDestructiveMigrationFrom(1, 2, 3, 4, 5)
                .allowMainThreadQueries().build();
    }

    /** Gets the icon path of a project for a specific project ID. **/
    @NonNull
    public static String getProjectIconPath(@NonNull Context context, String projectID) {
        return context.getFilesDir() + "/icons/project/" + projectID + ".jpg";
    }

    /** Generates a valid String based on its type. **/
    public static String generateValidProjectString(@NonNull RandomString rand, int type,
                                                    ProjectDatabase projectDatabase) {
        String returnString = rand.nextString();
        switch (type) {
            case ProjectCreateFragment.TYPE_PROJECT:
                while (projectDatabase.ProjectDao().searchByID(returnString) != null) {
                    returnString = rand.nextString();
                }
                break;
            case ProjectCreateFragment.TYPE_MEMBER:
                while (projectDatabase.MemberDao().searchByID(returnString) != null) {
                    returnString = rand.nextString();
                }
                break;
            case ProjectCreateFragment.TYPE_ROLE:
                while (projectDatabase.RoleDao().searchByID(returnString) != null) {
                    returnString = rand.nextString();
                }
                break;
            default:
                returnString = "";
        }
        return returnString;
    }
}