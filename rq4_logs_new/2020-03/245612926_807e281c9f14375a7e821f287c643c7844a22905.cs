using System;
using System.Collections.Generic;
using CourseGenerator.Models.Entities.Info;
using CourseGenerator.Models.Entities.InfoByThemes;
using CourseGenerator.Models.Entities.CourseAccess;

namespace CourseGenerator.Models
{
    public class TestData
    {
        #region Info block
        public static List<Language> Languages;
        public static List<Heading> Headings;
        public static List<HeadingLang> HeadingLangs;
        public static List<Level> Levels;
        public static List<LevelLang> LevelLangs;
        public static List<Competency> Competencies;
        public static List<CompetencyLang> CompetencyLangs;
        public static List<HeadingCompetency> HeadingCompetencies;
        public static List<MaterialType> MaterialTypes;
        public static List<MaterialTypeLang> MaterialTypeLangs;
        public static List<Material> Materials;
        public static List<MaterialLang> MaterialLangs;
        public static List<MaterialCompetency> MaterialCompetencies;
        public static List<HeadingMaterial> HeadingMaterials;
        #endregion

        #region InfoByThemes
        public static List<Course> Courses;
        public static List<CourseLang> CourseLangs;
        public static List<CourseDependency> CourseDependencies;
        public static List<Theme> Themes;
        public static List<ThemeLang> ThemeLangs;
        public static List<CourseHeading> CourseHeadings;
        public static List<ThemeHeading> ThemeHeadings;
        public static List<ThemeMaterial> ThemeMaterials;
        #endregion

        #region CourseAccess
        public static List<UserHeading> UserHeadings;
        public static List<UserCourse> UserCourses;
        public static List<UserTheme> UserThemes;
        #endregion

        static TestData()
        {
            InitializeInfoBlock();
            InitializeInfoByThemesBlock();
            InitializeCourseAccessBlock();
        }

        public static void InitializeInfoBlock()
        {
            Languages = new List<Language>();
            Headings = new List<Heading>();
            HeadingLangs = new List<HeadingLang>();
            Levels = new List<Level>();
            LevelLangs = new List<LevelLang>();
            Competencies = new List<Competency>();
            CompetencyLangs = new List<CompetencyLang>();
            HeadingCompetencies = new List<HeadingCompetency>();
            MaterialTypes = new List<MaterialType>();
            MaterialTypeLangs = new List<MaterialTypeLang>();
            Materials = new List<Material>();
            MaterialLangs = new List<MaterialLang>();
            MaterialCompetencies = new List<MaterialCompetency>();
            HeadingMaterials = new List<HeadingMaterial>();
        }

        public static void InitializeInfoByThemesBlock()
        {
            Courses = new List<Course>();
            CourseLangs = new List<CourseLang>();
            CourseDependencies = new List<CourseDependency>();
            Themes = new List<Theme>();
            ThemeLangs = new List<ThemeLang>();
            CourseHeadings = new List<CourseHeading>();
            ThemeHeadings = new List<ThemeHeading>();
            ThemeMaterials = new List<ThemeMaterial>();
        }

        public static void InitializeCourseAccessBlock()
        {
            UserHeadings = new List<UserHeading>();
            UserCourses = new List<UserCourse>();
            UserThemes = new List<UserTheme>();
        }
    }
}