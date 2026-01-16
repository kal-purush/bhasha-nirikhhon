import { describe, it, expect } from "vitest";
import parseYML from "../util/parse-playbook-yml.ts";

const testPlaybook = `playbook:
  goal: Understand structure, content, identifiers, and data generation process of incoming medical claims data.
  steps:
    - step: Determine primary patient identifier and other demographic fields.
      checks:
        - check: Identify potential patient identifier fields (e.g., member_id, patient_id, mbi, ssn).
        - check: Assess reliability and population consistency of identifier fields.
        - check: Determine primary source_local_patient_id candidate.
        - check: Note presence and overlap of multiple identifiers (e.g., Member ID and MBI).
        - check: Identify demographic fields (dob, gender, zip, state) and note formats/completeness.
        - check: Determine state code format (FIPS, SSA, Postal).

    - step: Distinguish Institutional vs. Professional claims.
      description: | 
        How do we determine which rows are professional claims and which rows are institutional claims? Sometimes 
        professional claims and institutional claims are kept in separate source data tables, in which case all of 
        the records on this table maybe one type or the other. But often these claims are intermingled in a table, 
        and each row is either one or the other. 
        There is often a claim type code with values such as 'I' and 'P'. Alternatively, type of bill codes are 
        only on institutional claims, whereas facility type codes are only on professional claims. If both of 
        these indicators are in place, make sure that they harmonize with each other.
      checks:
        - check: Check for an explicit claim_type_code field and its distinct values (e.g., 'I', 'P', 'institutional', 'professional').
          description: |
            For example, something like the following SQL
            select distinct claim_type_code
            from my_claims_table
            limit 20
        - check: Check presence and population of type_of_bill codes (indicative of Institutional).
        - check: Check presence and population of facility_type_code or place_of_service codes (indicative of Professional).
        - check: Verify harmonization between different indicators if multiple exist.
          description: |
            For example, 
            \`\`\`
            select count(*) as should_be_zero
            from my_claims_table
            where claim_type_code = 'I' -- Or equivalent institutional code
            and fac_type_code is not null
            and fac_type_code <> ''
            \`\`\`
            or
            \`\`\`
            select count(*) as should_be_zero
            from my_claims_table
            where claim_type_code = 'P' -- Or equivalent professional code
            and bill_type_code is not null
            and bill_type_code <> ''
            \`\`\`
        - check: Decide on the most reliable field(s) to determine claim class.
`;

describe("parsePlaybookYML", () => {
  it("parses multiline description", () => {
    const result = parseYML(testPlaybook);
    expect(result.goal).toBe("Understand structure, content, identifiers, and data generation process of incoming medical claims data.");
    expect(result.steps.length).toBe(2);
    expect(result.steps[0].name).toBe("Determine primary patient identifier and other demographic fields.");
    expect(result.steps[0].checks.length).toBe(6);
    expect(result.steps[1].name).toBe("Distinguish Institutional vs. Professional claims.");
    expect(result.steps[1].description).toContain(`How do we determine which rows are professional claims and which rows are institutional claims? Sometimes\nprofessional`);
  });
});