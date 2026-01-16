export const form = {
  "success": false,
  "message": "Form fetched successfully",
  "form": {
    "id": "3bd2d310-39e2-459d-8ca3-081097110831",
    "name": "Job Application Form",
    "description": null,
    "completed": false,
    "userId": "d23c47c0-d976-401c-ad37-bad10e92c535",
    "createdAt": "2025-05-21T02:45:23.060Z",
    "updatedAt": "2025-05-21T02:45:23.060Z",
    "sections": [
      {
        "id": "506225f9-998a-476b-9f33-904f0f3f2f87",
        "title": "Careers Profile",
        "order": 1,
        "formId": "3bd2d310-39e2-459d-8ca3-081097110831",
        "fields": [
          {
            "id": "e3650788-9b7f-4e5a-a730-cd262ca447b0",
            "label": "Resume",
            "fieldType": "file",
            "order": 1,
            "required": true,
            "placeholder": null,
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": []
          },
          {
            "id": "cae9a1bc-433a-4079-89ee-3b270b98e998",
            "label": "Full Name",
            "fieldType": "text",
            "order": 2,
            "required": true,
            "placeholder": "Enter your full name",
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": []
          },
          {
            "id": "203818fa-3cdb-4615-a40b-4e7a6566483f",
            "label": "Email",
            "fieldType": "email",
            "order": 3,
            "required": true,
            "placeholder": "A copy of your résumé has been attached, separate from the one you uploaded. If you change your résumé, you must upload it again.",
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": [
              {
                "id": "d216e995-7991-4369-9aec-2060bb24aa92",
                "label": "Primary Email Address",
                "type": "email",
                "order": 1,
                "required": true,
                "placeholder": "Enter your primary email",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "203818fa-3cdb-4615-a40b-4e7a6566483f"
              },
              {
                "id": "7de17287-f70f-479c-97d0-1c92f4dda84f",
                "label": "Secondary Email Address",
                "type": "email",
                "order": 2,
                "required": false,
                "placeholder": "Enter your secondary email",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "203818fa-3cdb-4615-a40b-4e7a6566483f"
              }
            ]
          },
          {
            "id": "0a7f3732-d107-4e1b-8b05-858d9c8e0a9e",
            "label": "Phone Number",
            "fieldType": "text",
            "order": 4,
            "required": true,
            "placeholder": "Enter your phone number",
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": []
          },
          {
            "id": "4ba47c85-8fa7-456d-ab8d-59eebdfbe300",
            "label": "Address",
            "fieldType": "text",
            "order": 5,
            "required": true,
            "placeholder": null,
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": [
              {
                "id": "485f8783-4245-4c67-9747-b8b3b68c4711",
                "label": "Street Address",
                "type": "text",
                "order": 1,
                "required": false,
                "placeholder": "Enter your street address",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "4ba47c85-8fa7-456d-ab8d-59eebdfbe300"
              },
              {
                "id": "f91d086c-4a7b-44d9-a5f5-63c86ebca731",
                "label": "City",
                "type": "text",
                "order": 2,
                "required": true,
                "placeholder": "Enter your city",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "4ba47c85-8fa7-456d-ab8d-59eebdfbe300"
              },
              {
                "id": "800149b5-2ce1-493c-b7ad-6c65e73bf82a",
                "label": "State",
                "type": "text",
                "order": 3,
                "required": false,
                "placeholder": "Enter your state",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "4ba47c85-8fa7-456d-ab8d-59eebdfbe300"
              },
              {
                "id": "14596142-4247-41e7-862d-b1d99f13e4c1",
                "label": "ZIP Code",
                "type": "text",
                "order": 4,
                "required": false,
                "placeholder": "Enter your ZIP code",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "4ba47c85-8fa7-456d-ab8d-59eebdfbe300"
              },
              {
                "id": "c8f7eaab-ea15-43f7-8aad-4233f5acf7e9",
                "label": "Country",
                "type": "text",
                "order": 5,
                "required": true,
                "placeholder": "Enter your country",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "4ba47c85-8fa7-456d-ab8d-59eebdfbe300"
              }
            ]
          },
          {
            "id": "1dfaf3e0-25c8-49f9-996e-c19c73b03e45",
            "label": "Higher Education",
            "fieldType": "group",
            "order": 6,
            "required": true,
            "placeholder": "Provide higher education details",
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": [
              {
                "id": "a56ea37f-b68c-4321-a9a4-176b006ed678",
                "label": "Attended university degree program?",
                "type": "radio",
                "order": 1,
                "required": true,
                "placeholder": null,
                "defaultValue": "Yes",
                "hint": null,
                "options": {
                  "choices": ["Yes", "No"]
                },
                "fieldId": "1dfaf3e0-25c8-49f9-996e-c19c73b03e45"
              },
              {
                "id": "0c6cff0e-6467-4fa4-a6fa-3f35b6ecd86c",
                "label": "School Name",
                "type": "text",
                "order": 2,
                "required": true,
                "placeholder": "Enter your university name",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "1dfaf3e0-25c8-49f9-996e-c19c73b03e45"
              },
              {
                "id": "9e6b9b67-667d-412e-a582-dd2906ad9478",
                "label": "Degree",
                "type": "select",
                "order": 3,
                "required": true,
                "placeholder": "Select your degree",
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": ["B.Tech", "B.Sc", "M.Tech", "MBA", "PhD"]
                },
                "fieldId": "1dfaf3e0-25c8-49f9-996e-c19c73b03e45"
              },
              {
                "id": "10cff1e1-d853-4259-9d27-a78ac45f3dc3",
                "label": "Degree Status",
                "type": "select",
                "order": 4,
                "required": true,
                "placeholder": "Select your degree status",
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": ["Completed", "Ongoing", "Dropped"]
                },
                "fieldId": "1dfaf3e0-25c8-49f9-996e-c19c73b03e45"
              },
              {
                "id": "927b80d9-52e4-4b05-b9aa-338015279b3a",
                "label": "Major / area of study",
                "type": "text",
                "order": 5,
                "required": true,
                "placeholder": "e.g., Computer Science",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "1dfaf3e0-25c8-49f9-996e-c19c73b03e45"
              },
              {
                "id": "91b9e6d2-54cf-4ed6-aac1-d33850422f24",
                "label": "Country / Region",
                "type": "select",
                "order": 6,
                "required": true,
                "placeholder": "Select country",
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": ["India", "USA", "UK", "Canada"]
                },
                "fieldId": "1dfaf3e0-25c8-49f9-996e-c19c73b03e45"
              }
            ]
          },
          {
            "id": "deb39764-81a7-4ba5-86b0-862f60cc9432",
            "label": "Work Experience",
            "fieldType": "group",
            "order": 7,
            "required": true,
            "placeholder": "Provide your most recent work experience",
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": [
              {
                "id": "184a6675-bd2c-48f6-a5ee-2f3c0ccb808a",
                "label": "Applying for your first job?",
                "type": "radio",
                "order": 1,
                "required": true,
                "placeholder": null,
                "defaultValue": "No",
                "hint": null,
                "options": {
                  "choices": ["Yes", "No"]
                },
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "47663fba-c5be-4691-a565-18f804554c70",
                "label": "Employer name",
                "type": "text",
                "order": 2,
                "required": true,
                "placeholder": "Enter your employer name",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "c47ccb78-c7ba-4290-b25a-82fd5327ecdc",
                "label": "Job title",
                "type": "text",
                "order": 3,
                "required": true,
                "placeholder": "Enter your job title",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "57a1e545-aa6f-4f3a-b2c4-608d4784edf1",
                "label": "Start Month",
                "type": "select",
                "order": 4,
                "required": true,
                "placeholder": "Select start month",
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": [
                    "January",
                    "February",
                    "March",
                    "April",
                    "May",
                    "June",
                    "July",
                    "August",
                    "September",
                    "October",
                    "November",
                    "December"
                  ]
                },
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "b8398ba8-fdee-44c7-9a0d-ca6c72d65b28",
                "label": "Start Year",
                "type": "number",
                "order": 5,
                "required": true,
                "placeholder": "Enter start year",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "d9cee67b-9379-46a9-b402-52b625accb26",
                "label": "This is your current job",
                "type": "checkbox",
                "order": 6,
                "required": false,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "4cc57796-1d7f-48e1-a1bb-665a1b5dce76",
                "label": "End Month",
                "type": "select",
                "order": 7,
                "required": true,
                "placeholder": "Select end month",
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": [
                    "January",
                    "February",
                    "March",
                    "April",
                    "May",
                    "June",
                    "July",
                    "August",
                    "September",
                    "October",
                    "November",
                    "December"
                  ]
                },
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "afb7637a-f075-4eb8-8862-c414e15726cb",
                "label": "End Year",
                "type": "number",
                "order": 8,
                "required": true,
                "placeholder": "Enter end year",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "d6d97d84-3242-43d9-aa90-76f67485c8fc",
                "label": "Country / Region",
                "type": "select",
                "order": 9,
                "required": true,
                "placeholder": "Select country",
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": ["India", "USA", "UK", "Canada"]
                },
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "8d340796-d0f3-4ec7-98d3-efbad1a69054",
                "label": "City",
                "type": "text",
                "order": 10,
                "required": true,
                "placeholder": "Enter city",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              },
              {
                "id": "9804823d-e0cc-4c38-bf7f-5a1ddb0dab31",
                "label": "State",
                "type": "text",
                "order": 11,
                "required": false,
                "placeholder": "Enter state",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "deb39764-81a7-4ba5-86b0-862f60cc9432"
              }
            ]
          },
          {
            "id": "826216d0-d4de-4573-8ec4-a371c8962684",
            "label": "Social Links",
            "fieldType": "group",
            "order": 8,
            "required": false,
            "placeholder": "Add your professional social links",
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": [
              {
                "id": "c09de907-8bf6-4e56-b181-1da937bad6e3",
                "label": "LinkedIn Profile",
                "type": "text",
                "order": 1,
                "required": false,
                "placeholder": "Paste your LinkedIn profile URL",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "826216d0-d4de-4573-8ec4-a371c8962684"
              },
              {
                "id": "b451c1a0-d955-4870-b210-66b77ac9d483",
                "label": "GitHub Profile",
                "type": "text",
                "order": 2,
                "required": false,
                "placeholder": "Paste your GitHub profile URL",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "826216d0-d4de-4573-8ec4-a371c8962684"
              },
              {
                "id": "2bab63c5-8bc8-4fc0-8ef5-fce4762b4371",
                "label": "Portfolio Website",
                "type": "text",
                "order": 3,
                "required": false,
                "placeholder": "Paste your portfolio website URL",
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "826216d0-d4de-4573-8ec4-a371c8962684"
              }
            ]
          },
          {
            "id": "bcc9ca92-156f-4eed-a971-b6d90cc556e9",
            "label": "Consent & Certification",
            "fieldType": "group",
            "order": 9,
            "required": true,
            "placeholder": "Please review and acknowledge the following statements",
            "sectionId": "506225f9-998a-476b-9f33-904f0f3f2f87",
            "subField": [
              {
                "id": "b10c6d92-9b4f-4eaa-a9f4-2fea5eb1a1a0",
                "label": "I consent to the processing of my information as described in Google's applicant and candidate privacy policy. In limited circumstances, Google may share my contact information with trusted third parties to assist in certain phases of the hiring process (such as conducting background checks).",
                "type": "checkbox",
                "order": 1,
                "required": true,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "bcc9ca92-156f-4eed-a971-b6d90cc556e9"
              },
              {
                "id": "e1a74d20-2580-4997-a384-909c82715066",
                "label": "I hereby certify that, to the best of my knowledge, the provided information is true and accurate.",
                "type": "checkbox",
                "order": 2,
                "required": true,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": null,
                "fieldId": "bcc9ca92-156f-4eed-a971-b6d90cc556e9"
              }
            ]
          }
        ]
      },
      {
        "id": "db10ee5e-6a1b-4ff1-a749-504b5d6435f0",
        "title": "Role Information",
        "order": 2,
        "formId": "3bd2d310-39e2-459d-8ca3-081097110831",
        "fields": [
          {
            "id": "94c400f4-410f-49e0-91f4-8c3ddadcd83c",
            "label": "Minimum Qualifications",
            "fieldType": "group",
            "order": 1,
            "required": true,
            "placeholder": "Confirm your qualifications",
            "sectionId": "db10ee5e-6a1b-4ff1-a749-504b5d6435f0",
            "subField": [
              {
                "id": "7c4dae65-4a33-4831-9784-cdadd2a78941",
                "label": "Do you have a Bachelor's degree or equivalent practical experience?",
                "type": "radio",
                "order": 1,
                "required": true,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": ["Yes", "No", "Not sure"]
                },
                "fieldId": "94c400f4-410f-49e0-91f4-8c3ddadcd83c"
              }
            ]
          },
          {
            "id": "2006a3f4-a4df-463e-91ec-f9ed957aeb2b",
            "label": "Work Authorization",
            "fieldType": "group",
            "order": 2,
            "required": true,
            "placeholder": "Confirm your eligibility",
            "sectionId": "db10ee5e-6a1b-4ff1-a749-504b5d6435f0",
            "subField": [
              {
                "id": "b7e5302e-58fb-4759-b2b7-997e30aac541",
                "label": "Are you legally eligible to work in the country of employment?",
                "type": "radio",
                "order": 1,
                "required": true,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": ["Yes", "No"]
                },
                "fieldId": "2006a3f4-a4df-463e-91ec-f9ed957aeb2b"
              },
              {
                "id": "40841079-0a13-4677-91e8-39add389fa47",
                "label": "Do you currently need, or will you someday require, company to sponsor work authorization for you to work in the country of employment?",
                "type": "radio",
                "order": 2,
                "required": true,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": ["Yes", "No"]
                },
                "fieldId": "2006a3f4-a4df-463e-91ec-f9ed957aeb2b"
              }
            ]
          },
          {
            "id": "c5bc8229-68ff-4a7e-99dc-ae07571ec2e9",
            "label": "Years of Experience",
            "fieldType": "number",
            "order": 3,
            "required": true,
            "placeholder": "Enter years of experience",
            "sectionId": "db10ee5e-6a1b-4ff1-a749-504b5d6435f0",
            "subField": []
          },
          {
            "id": "0dba50fc-4108-40e0-b237-26a1a4029b1e",
            "label": "Expected Salary",
            "fieldType": "number",
            "order": 4,
            "required": false,
            "placeholder": "Enter expected salary in inr",
            "sectionId": "db10ee5e-6a1b-4ff1-a749-504b5d6435f0",
            "subField": []
          }
        ]
      },
      {
        "id": "2a7e0909-5ae2-4d59-b755-4e470b12350e",
        "title": "Voluntary Self-Identification",
        "order": 3,
        "formId": "3bd2d310-39e2-459d-8ca3-081097110831",
        "fields": [
          {
            "id": "cfc7a927-bdf8-469d-b2f3-25a2e706cebd",
            "label": "Gender",
            "fieldType": "group",
            "order": 1,
            "required": false,
            "placeholder": "",
            "sectionId": "2a7e0909-5ae2-4d59-b755-4e470b12350e",
            "subField": [
              {
                "id": "25080a77-a247-4879-876a-d5fafea9e307",
                "label": "Gender",
                "type": "radio",
                "order": 1,
                "required": false,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": [
                    "Male",
                    "Female",
                    "Non-binary",
                    "Prefer not to say"
                  ]
                },
                "fieldId": "cfc7a927-bdf8-469d-b2f3-25a2e706cebd"
              }
            ]
          },
          {
            "id": "897bf1f9-0ca1-4096-8fb9-fb4f5e4313fa",
            "label": "Ethnicity",
            "fieldType": "group",
            "order": 2,
            "required": false,
            "placeholder": "Select your ethnicity",
            "sectionId": "2a7e0909-5ae2-4d59-b755-4e470b12350e",
            "subField": [
              {
                "id": "81ceae2a-9938-4028-bbe3-16ed5c52ad71",
                "label": "Ethnicity",
                "type": "select",
                "order": 1,
                "required": false,
                "placeholder": "Select your ethnicity",
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": [
                    "Asian",
                    "Black or African American",
                    "Hispanic or Latino",
                    "White",
                    "Other",
                    "Prefer not to say"
                  ]
                },
                "fieldId": "897bf1f9-0ca1-4096-8fb9-fb4f5e4313fa"
              }
            ]
          },
          {
            "id": "a476689c-45f8-4732-b0af-aa206456b8c8",
            "label": "Disability Status",
            "fieldType": "group",
            "order": 3,
            "required": false,
            "placeholder": "",
            "sectionId": "2a7e0909-5ae2-4d59-b755-4e470b12350e",
            "subField": [
              {
                "id": "c4c78117-1191-4431-8828-53aeefbcf4a8",
                "label": "Disability Status",
                "type": "radio",
                "order": 1,
                "required": false,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": [
                    "Yes, I have a disability",
                    "No, I do not have a disability",
                    "Prefer not to say"
                  ]
                },
                "fieldId": "a476689c-45f8-4732-b0af-aa206456b8c8"
              }
            ]
          },
          {
            "id": "9b18afbe-0949-4ff7-83b0-60b6b356eb90",
            "label": "Veteran Status",
            "fieldType": "group",
            "order": 4,
            "required": false,
            "placeholder": "",
            "sectionId": "2a7e0909-5ae2-4d59-b755-4e470b12350e",
            "subField": [
              {
                "id": "3c9ee092-0e3a-42e4-8106-314c0cb78434",
                "label": "Veteran Status",
                "type": "radio",
                "order": 1,
                "required": false,
                "placeholder": null,
                "defaultValue": null,
                "hint": null,
                "options": {
                  "choices": [
                    "I am a veteran",
                    "I am not a veteran",
                    "Prefer not to say"
                  ]
                },
                "fieldId": "9b18afbe-0949-4ff7-83b0-60b6b356eb90"
              }
            ]
          }
        ]
      },
      {
        "id": "e6e63182-5c2b-4138-8b4b-09dd706f603c",
        "title": "Review & Apply",
        "order": 4,
        "formId": "3bd2d310-39e2-459d-8ca3-081097110831",
        "fields": [
          {
            "id": "703c5456-3d30-4a08-99d5-03191c88c173",
            "label": "Review Your Information",
            "fieldType": "review",
            "order": 1,
            "required": false,
            "placeholder": "",
            "sectionId": "e6e63182-5c2b-4138-8b4b-09dd706f603c",
            "subField": []
          },
          {
            "id": "cf23d4c7-0783-4733-805b-af9d53aabf3a",
            "label": "Confirm and Submit",
            "fieldType": "checkbox",
            "order": 2,
            "required": true,
            "placeholder": "",
            "sectionId": "e6e63182-5c2b-4138-8b4b-09dd706f603c",
            "subField": [
              {
                "id": "3dec03c6-35c9-4476-bcf3-e90aa518f239",
                "label": "I confirm that the information provided is accurate",
                "type": "checkbox",
                "order": 1,
                "required": true,
                "placeholder": null,
                "defaultValue": "true",
                "hint": null,
                "options": null,
                "fieldId": "cf23d4c7-0783-4733-805b-af9d53aabf3a"
              }
            ]
          }
        ]
      }
    ]
  }
}