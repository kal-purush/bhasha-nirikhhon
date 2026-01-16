import { EducationPrograms } from "~/app/(landing-routes)/courses/types/index.types";
import useCoursesStore from "~/stores/course.store";
import { Course } from "./services.type";

/* eslint-disable unicorn/no-null */
const BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3000";

export async function getCourseData(slug: string): Promise<EducationPrograms> {
  const response = await fetch(
    `${process.env.NEXT_PUBLIC_LOCAL_URL}/api/courses.json`,
    {
      cache: "no-store",
    },
  );

  if (!response.ok) {
    throw new Error("Failed to fetch data");
  }

  const data = await response.json();

  const course = data.find(
    (course: object) =>
      Object.keys(course)[0].toLowerCase() === slug.toLowerCase(),
  );

  if (!course) {
    throw new Error("Course not found");
  }

  return course[slug];
}

export const fetchAllCourses = async () => {
  const { setCourses, setLoading, setError } = useCoursesStore.getState();

  setLoading(true);
  setError(null);

  try {
    const response = await fetch(`${BASE_URL}/external/courses`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
      cache: "no-store",
    });

    if (!response.ok) {
      throw new Error(`Error: ${response.statusText}`);
    }

    const data = await response.json();
    setCourses(data.data);
  } catch (error: unknown) {
    if (error instanceof Error) {
      setError(error.message);
    } else {
      setError("An unknown error occurred");
    }
  } finally {
    setLoading(false); // Ensure this line is present
  }
};

// Function to set the active course
export const setActiveCourse = (course: Course) => {
  const { setActiveCourse } = useCoursesStore.getState();
  setActiveCourse(course);
};