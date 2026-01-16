using DataLayer;
using System.Data.SqlClient;

namespace ApiConsole
{
    internal class AddDataToSQL
    {
        #region Insert Work Items
        public static async Task InsertWorkItemsIntoDatabaseAsync(string connectionString, List<WorkItem> workItems)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                foreach (var workItem in workItems)
                {
                    string query = @"
                INSERT INTO WorkItems (
                    WorkItemId, Title, AssignedTo, State, Tags, TeamProject, WorkItemType, Round, Discipline, RequirementCategory, OriginalRequirementId
                ) VALUES (
                    @WorkItemId, @Title, @AssignedTo, @State, @Tags, @TeamProject, @WorkItemType, @Round, @Discipline, @RequirementCategory, @OriginalRequirementId
                )";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@WorkItemId", workItem.Id);
                        command.Parameters.AddWithValue("@Title", workItem.Title);
                        command.Parameters.AddWithValue("@AssignedTo", workItem.AssignedTo ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@State", workItem.State ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@Tags", workItem.Tags ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@TeamProject", workItem.TeamProject ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@WorkItemType", workItem.WorkItemType ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@Round", workItem.Round ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@Discipline", workItem.Discipline ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@RequirementCategory", workItem.RequirementCategory ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@OriginalRequirementId", workItem.OriginalRequirementId ?? (object)DBNull.Value);

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }
        #endregion

        #region Insert Bugs
        public static async Task InsertBugsIntoDatabaseAsync(string connectionString, List<AdoBug> bugs)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                foreach (var bug in bugs)
                {
                    string query = @"
                INSERT INTO Bugs (
                    BugId, Title, State, BugType, Priority
                ) VALUES (
                    @BugId, @Title, @State, @BugType, @Priority
                )";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@BugId", bug.Id);
                        command.Parameters.AddWithValue("@Title", bug.Title);
                        command.Parameters.AddWithValue("@State", bug.State ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@BugType", bug.BugType ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@Priority", bug.Priority);

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }
        #endregion

        #region Insert Test Cases
        public static async Task InsertTestCasesIntoDatabaseAsync(string connectionString, List<TestCase> testCases)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                foreach (var testCase in testCases)
                {
                    string query = @"
                INSERT INTO TestCases (
                    TestCaseId, Title, State
                ) VALUES (
                    @TestCaseId, @Title, @State
                )";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@TestCaseId", testCase.Id);
                        command.Parameters.AddWithValue("@Title", testCase.Title);
                        command.Parameters.AddWithValue("@State", testCase.State ?? (object)DBNull.Value);

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }
        #endregion

        #region Insert Requirements
        public static async Task InsertRequirementsIntoDatabaseAsync(string connectionString, List<Requirement> requirements)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                foreach (var requirement in requirements)
                {
                    string query = @"
                INSERT INTO Requirements (
                    RequirementId, Title, AssignedTo, State, TestVerificationMethod, BoardColumn
                ) VALUES (
                    @RequirementId, @Title, @AssignedTo, @State, @TestVerificationMethod, @BoardColumn
                )";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@RequirementId", requirement.Id);
                        command.Parameters.AddWithValue("@Title", requirement.Title);
                        command.Parameters.AddWithValue("@AssignedTo", requirement.AssignedTo ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@State", requirement.State ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@TestVerificationMethod", requirement.TestVerificationMethod ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@BoardColumn", requirement.BoardColumn ?? (object)DBNull.Value);

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }
        #endregion

        #region Insert Outcomes
        public static async Task InsertOutcomesIntoDatabaseAsync(string connectionString, List<Outcome> outcomes)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                foreach (var outcome in outcomes)
                {
                    string query = @"
                        INSERT INTO Outcomes (
                                OutcomeId, Title, AssignedTo, State, CurrentOutcome, ChangedDate, ChangedBy
                            ) VALUES (
                                @OutcomeId, @Title, @AssignedTo, @State, @CurrentOutcome, @ChangedDate, @ChangedBy
                            )";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@OutcomeId", outcome.Id);
                        command.Parameters.AddWithValue("@Title", outcome.Title ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@AssignedTo", outcome.AssignedTo ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@State", outcome.State ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@CurrentOutcome", outcome.CurrentOutcome ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@ChangedDate", outcome.ChangedDate != DateTime.MinValue ? (object)outcome.ChangedDate : DBNull.Value);
                        command.Parameters.AddWithValue("@ChangedBy", outcome.ChangedBy ?? (object)DBNull.Value);

                        try
                        {
                            await command.ExecuteNonQueryAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error inserting outcome with ID: {outcome.Id}, Error: {ex.Message}");
                        }
                    }
                }
            }
        }
        #endregion
    }
}