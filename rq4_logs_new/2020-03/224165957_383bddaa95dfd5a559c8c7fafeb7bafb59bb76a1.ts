import { Heading, HeadingLevels } from "lbh-frontend-react";
import {
  ComponentDatabaseMap,
  ComponentWrapper,
  DynamicComponent,
  StaticComponent
} from "remultiform/component-wrapper";
import { makeSubmit } from "../../components/makeSubmit";
import { TextInput } from "../../components/TextInput";
import keyFromSlug from "../../helpers/keyFromSlug";
import ProcessStepDefinition from "../../helpers/ProcessStepDefinition";
import ResidentDatabaseSchema from "../../storage/ResidentDatabaseSchema";
import Storage from "../../storage/Storage";
import PageSlugs from "../PageSlugs";
import PageTitles from "../PageTitles";

const questions = {
  "other-support-details": "Other support details"
};

const step: ProcessStepDefinition<ResidentDatabaseSchema, "otherSupport"> = {
  title: PageTitles.OtherSupport,
  heading: "Other Support",
  context: Storage.ResidentContext,
  review: {
    rows: [
      {
        label: questions["other-support-details"],
        values: {
          "other-support-full-name": {
            renderValue(name: string): React.ReactNode {
              return name;
            }
          },
          "other-support-role": {
            renderValue(role: string): React.ReactNode {
              return role;
            }
          },
          "other-support-phone-number": {
            renderValue(phoneNumber: string): React.ReactNode {
              return phoneNumber;
            }
          }
        }
      }
    ]
  },
  step: {
    slug: PageSlugs.OtherSupport,
    nextSlug: PageSlugs.Verify,
    submit: (nextSlug?: string): ReturnType<typeof makeSubmit> =>
      makeSubmit({
        slug: nextSlug as PageSlugs | undefined,
        value: "Save and continue"
      }),
    componentWrappers: [
      ComponentWrapper.wrapStatic<ResidentDatabaseSchema, "otherSupport">(
        new StaticComponent({
          key: "other-support-heading",
          Component: Heading,
          props: {
            level: HeadingLevels.H2,
            children: questions["other-support-details"]
          }
        })
      ),
      ComponentWrapper.wrapDynamic(
        new DynamicComponent({
          key: "other-support-full-name",
          Component: TextInput,
          props: {
            name: "other-support-full-name",
            label: "Full name"
          },
          defaultValue: "",
          emptyValue: "",
          databaseMap: new ComponentDatabaseMap<
            ResidentDatabaseSchema,
            "otherSupport"
          >({
            storeName: "otherSupport",
            key: keyFromSlug(),
            property: ["fullName"]
          })
        })
      ),
      ComponentWrapper.wrapDynamic(
        new DynamicComponent({
          key: "other-support-role",
          Component: TextInput,
          props: {
            name: "other-support-role",
            label: "Role (e.g. support worker, social worker, doctor)"
          },
          defaultValue: "",
          emptyValue: "",
          databaseMap: new ComponentDatabaseMap<
            ResidentDatabaseSchema,
            "otherSupport"
          >({
            storeName: "otherSupport",
            key: keyFromSlug(),
            property: ["role"]
          })
        })
      ),
      ComponentWrapper.wrapDynamic(
        new DynamicComponent({
          key: "other-support-phone-number",
          Component: TextInput,
          props: {
            name: "other-support-phone-number",
            label: "Phone number"
          },
          defaultValue: "",
          emptyValue: "",
          databaseMap: new ComponentDatabaseMap<
            ResidentDatabaseSchema,
            "otherSupport"
          >({
            storeName: "otherSupport",
            key: keyFromSlug(),
            property: ["phoneNumber"]
          })
        })
      )
    ]
  }
};

export default step;