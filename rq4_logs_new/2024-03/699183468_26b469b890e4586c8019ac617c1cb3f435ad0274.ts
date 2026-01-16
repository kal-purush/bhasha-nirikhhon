import { test, expect } from "@playwright/test"
import { setupPlaywright } from "@test-utils/setup-playwright"
import { mockRequest } from "@test-utils/mock-request"
import { loginUser } from "@test-utils/login-user"

setupPlaywright()

test.describe("Admin - Edit Survey Administration", () => {
  test.beforeEach(async ({ page }) => {
    await mockRequest(page, "/auth/refresh", {
      status: 403,
      contentType: "application/json",
      body: JSON.stringify({ message: "Forbidden" }),
    })
    await mockRequest(page, "/gsi/client", {
      status: 200,
      contentType: "application/json",
    })
  })

  test.describe("as Guest", () => {
    test("should not allow to view edit survey administration", async ({ page }) => {
      await page.goto("/admin/survey-administrations/1/edit")

      await expect(page).toHaveURL("/auth/login?callback=/admin/survey-administrations/1/edit")
    })
  })

  test.describe("as Employee", () => {
    test("should not allow to view edit survey administration", async ({ page }) => {
      await loginUser("employee", page)

      await page.goto("/admin/survey-administrations/1/edit")

      await mockRequest(page, "/user/my-evaluations", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          data: [],
          pageInfo: {
            hasPreviousPage: false,
            hasNextPage: false,
            totalPages: 0,
          },
        }),
      })

      await mockRequest(
        page,
        "/user/email-templates?template_type=No+Available+Evaluation+Results",
        {
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            id: 11,
            name: "No Available Evaluation Results",
            template_type: "No Available Evaluation Results",
            is_default: true,
            subject: "",
            content:
              "Uh-oh! 🤷‍♂️\n\nLooks like our magical elves are still working their charm behind the scenes, and your results haven't arrived just yet. Don't worry, though – good things come to those who wait!\n\nIn the meantime, why not grab a cup of coffee or practice your superhero pose? 🦸‍♀️ We'll have those results ready for you in no time.\n\nStay tuned and keep the positive vibes flowing! ✨",
            created_by_id: null,
            updated_by_id: null,
            created_at: null,
            updated_at: null,
          }),
        }
      )

      await mockRequest(page, "/user/score-ratings", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            created_at: null,
            display_name: "Navigational Challenge",
            evaluee_description:
              "You face occasional difficulty in navigating job responsibilities.\nYour performance consistently falls below expectations and significant improvement is needed in various aspects of job responsibilities.\nYour goals and objectives are not met consistently.",
            id: 1,
            max_score: "1.99",
            min_score: "0",
            name: "Needs Improvement",
            result_description:
              "Employee faces occasional difficulty in navigating job responsibilities.\nPerformance consistently falls below expectations and significant improvement is needed in various aspects of job responsibilities.\nGoals and objectives are not met consistently.",
            status: null,
            updated_at: null,
          },
        ]),
      })

      await page.waitForLoadState("networkidle")

      await expect(page).toHaveURL("/my-evaluations")
    })
  })

  test.describe("as Admin", () => {
    test("should render correctly", async ({ page, isMobile }) => {
      await loginUser("admin", page)

      await page.goto("/admin/survey-administrations/1/edit")

      await mockRequest(page, "/admin/survey-administrations/1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: "Survey 1",
          survey_start_date: "2024-01-01T00:00:00.000Z",
          survey_end_date: "2024-01-03T00:00:00.000Z",
          remarks: "Remarks 1",
          email_subject: "Subject 1",
          email_content: "Content 1",
          status: "Draft",
        }),
      })

      await mockRequest(page, "/user/email-templates?template_type=Create+Survey", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: null,
          template_type: "Create Survey",
          is_default: true,
          subject: "Subject 1",
          content: "Content 1",
          created_by_id: null,
          updated_by_id: null,
          created_at: null,
          updated_at: null,
        }),
      })

      await mockRequest(page, "/admin/survey-templates/all", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            id: 1,
            name: "Template name",
            display_name: "Template display name",
            survey_type: "Create Survey type",
            is_active: true,
            remarks: "remarks",
          },
        ]),
      })

      if (isMobile) {
        await page.getByTestId("SidebarCloseButton").click()
      }

      await expect(page.getByRole("heading", { name: "Edit Survey" })).toBeVisible()
      await expect(page.getByPlaceholder("Name")).toBeVisible()

      await expect(page.locator("#survey_schedule")).toBeVisible()
      await expect(page.locator("#template")).toBeVisible()

      await expect(page.getByLabel("Description")).toBeVisible()

      await expect(page.getByPlaceholder("Subject")).toHaveValue("Subject 1")
      await expect(page.getByPlaceholder("Email content")).toHaveValue("Content 1")

      await expect(page.getByRole("button", { name: "Cancel & Exit" })).toBeVisible()
      await expect(page.getByRole("button", { name: "Save & Proceed" })).toBeVisible()
    })

    test("should show validation errors", async ({ page, isMobile }) => {
      await loginUser("admin", page)

      await page.goto("/admin/survey-administrations/1/edit")

      await mockRequest(page, "/admin/survey-administrations/1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: "",
          survey_start_date: "2024-01-01T00:00:00.000Z",
          survey_end_date: "2024-01-03T00:00:00.000Z",
          remarks: "",
          email_subject: "Subject 1",
          email_content: "Content 1",
          status: "Draft",
        }),
      })

      await mockRequest(page, "/user/email-templates?template_type=Create+Survey", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: null,
          template_type: "Create Survey",
          is_default: true,
          subject: "Subject 1",
          content: "Content 1",
          created_by_id: null,
          updated_by_id: null,
          created_at: null,
          updated_at: null,
        }),
      })

      await mockRequest(page, "/admin/survey-templates/all", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            id: 1,
            name: "Template name",
            display_name: "Template display name",
            survey_type: "Create Survey type",
            is_active: true,
            remarks: "remarks",
          },
        ]),
      })

      if (isMobile) {
        await page.getByTestId("SidebarCloseButton").click()
      }

      if (!isMobile) {
        await page.getByRole("button", { name: "Save" }).click()
        await expect(page.getByText("Name is required")).toBeVisible()
        await expect(page.getByText("Description is required")).toBeVisible()
      }
    })

    test("should edit evaluation successfully", async ({ page, isMobile }) => {
      await loginUser("admin", page)

      await page.goto("/admin/survey-administrations/1/edit")

      await mockRequest(page, "/admin/survey-administrations/1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: "Survey 1",
          survey_start_date: "2024-01-01T00:00:00.000Z",
          survey_end_date: "2024-01-03T00:00:00.000Z",
          remarks: "Remarks 1",
          email_subject: "Subject 1",
          email_content: "Content 1",
          status: "Draft",
        }),
      })

      await mockRequest(page, "/user/email-templates?template_type=Create+Survey", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: null,
          template_type: "Create Survey",
          is_default: true,
          subject: "Subject 1",
          content: "Content 1",
          created_by_id: null,
          updated_by_id: null,
          created_at: null,
          updated_at: null,
        }),
      })

      await mockRequest(page, "/admin/survey-templates/all", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            id: 1,
            name: "Template name",
            display_name: "Template display name",
            survey_type: "Create Survey type",
            is_active: true,
            remarks: "remarks",
          },
        ]),
      })

      if (isMobile) {
        await page.getByTestId("SidebarCloseButton").click()
      }

      await page.getByPlaceholder("Name").fill("Survey Edited")
      await page.getByLabel("Description").fill("Description Edited")
      await page
        .locator("div")
        .filter({ hasText: /^Select\.\.\.$/ })
        .nth(1)
        .click()
      await page.getByText("Template display name", { exact: true }).click()

      await mockRequest(page, "/admin/survey-administrations/1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({}),
      })

      await mockRequest(page, "/admin/evaluation-results?evaluation_administration_id=1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          data: [],
          pageInfo: {
            hasPreviousPage: false,
            hasNextPage: false,
            totalPages: 1,
          },
        }),
      })

      await mockRequest(page, "/admin/survey-administrations/1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: "Survey 1",
          survey_start_date: "2024-01-01T00:00:00.000Z",
          survey_end_date: "2024-01-03T00:00:00.000Z",
          remarks: "Remarks 1",
          email_subject: "Subject 1",
          email_content: "Content 1",
          status: "Draft",
        }),
      })

      await mockRequest(page, "/admin/survey-results/all?survey_administration_id=1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            id: 1,
            status: "For Review",
            users: {
              first_name: "Sample",
              last_name: "User",
              picture: null,
            },
          },
        ]),
      })

      await page.getByRole("button", { name: "Save & Proceed" }).click()

      await page.waitForLoadState("networkidle")

      await expect(page).toHaveURL("/admin/survey-administrations/1")
    })

    test("should render cancel & exit modal correctly", async ({ page, isMobile }) => {
      await loginUser("admin", page)

      await page.goto("/admin/survey-administrations/1/edit")

      await mockRequest(page, "/admin/survey-administrations/1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: "Survey 1",
          survey_start_date: "2024-01-01T00:00:00.000Z",
          survey_end_date: "2024-01-03T00:00:00.000Z",
          remarks: "Remarks 1",
          email_subject: "Subject 1",
          email_content: "Content 1",
          status: "Draft",
        }),
      })

      await mockRequest(page, "/user/email-templates?template_type=Create+Survey", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: null,
          template_type: "Create Survey",
          is_default: true,
          subject: "Subject 1",
          content: "Content 1",
          created_by_id: null,
          updated_by_id: null,
          created_at: null,
          updated_at: null,
        }),
      })

      await mockRequest(page, "/admin/survey-templates/all", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            id: 1,
            name: "Template name",
            display_name: "Template display name",
            survey_type: "Create Survey type",
            is_active: true,
            remarks: "remarks",
          },
        ]),
      })

      if (isMobile) {
        await page.getByTestId("SidebarCloseButton").click()
      }

      await page.getByRole("button", { name: "Cancel & Exit" }).click()

      await expect(page.getByRole("heading", { name: "Cancel & Exit" })).toBeVisible()
      await expect(
        page.getByText(
          "Are you sure you want to cancel and exit? If you cancel, your data won't be save"
        )
      ).toBeVisible()
      await expect(page.getByRole("button", { name: "No" })).toBeVisible()
      await expect(page.getByRole("button", { name: "Yes" })).toBeVisible()
    })

    test("should allow to cancel & exit", async ({ page, isMobile }) => {
      await loginUser("admin", page)

      await page.goto("/admin/survey-administrations/1/edit")

      await mockRequest(page, "/admin/survey-administrations/1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: "Survey 1",
          survey_start_date: "2024-01-01T00:00:00.000Z",
          survey_end_date: "2024-01-03T00:00:00.000Z",
          remarks: "Remarks 1",
          email_subject: "Subject 1",
          email_content: "Content 1",
          status: "Draft",
        }),
      })

      await mockRequest(page, "/user/email-templates?template_type=Create+Survey", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: null,
          template_type: "Create Survey",
          is_default: true,
          subject: "Subject 1",
          content: "Content 1",
          created_by_id: null,
          updated_by_id: null,
          created_at: null,
          updated_at: null,
        }),
      })

      await mockRequest(page, "/admin/survey-templates/all", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            id: 1,
            name: "Template name",
            display_name: "Template display name",
            survey_type: "Create Survey type",
            is_active: true,
            remarks: "remarks",
          },
        ]),
      })

      if (isMobile) {
        await page.getByTestId("SidebarCloseButton").click()
      }

      await page.getByRole("button", { name: "Cancel & Exit" }).click()
      await page.getByRole("button", { name: "Yes" }).click()

      await mockRequest(page, "/admin/survey-administrations/1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: 1,
          name: "Survey 1",
          survey_start_date: "2024-01-01T00:00:00.000Z",
          survey_end_date: "2024-01-03T00:00:00.000Z",
          remarks: "Remarks 1",
          email_subject: "Subject 1",
          email_content: "Content 1",
          status: "Draft",
        }),
      })

      await mockRequest(page, "/admin/survey-results/all?survey_administration_id=1", {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            id: 1,
            status: "For Review",
            users: {
              first_name: "Sample",
              last_name: "User",
              picture: null,
            },
          },
        ]),
      })

      await page.waitForLoadState("networkidle")

      await expect(page).toHaveURL("/admin/survey-administrations/1")
    })
  })
})