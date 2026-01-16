import { PromoTemplate, Promo } from '../../types/index';

const entity = ({ R }) => {
  return async function promo({
    name,
    template,
    title,
    description,
    status,
    minimumBalance,
    requiredMemberFields,
  }): Promise<Promo> {
    if (!name) {
      throw new Error(`Please input name`);
    }

    if (!template) {
      throw new Error(`Please input template`);
    }

    if (!title) {
      throw new Error(`Please input title`);
    }

    if (!description) {
      throw new Error(`Please input description`);
    }

    if (
      template !== PromoTemplate.Deposit &&
      template !== PromoTemplate.SignUp
    ) {
      throw new Error(`Invalid template`);
    }

    if (template === PromoTemplate.SignUp) {
      if (requiredMemberFields) {
        throw new Error(
          `Invalid input field: requiredMemberFields for sign up`,
        );
      }

      if (!minimumBalance) {
        throw new Error(`Please input minimum balance`);
      }
    }

    if (template === PromoTemplate.Deposit) {
      if (minimumBalance) {
        throw new Error(`Invalid input field: minimumBalance for deposit`);
      }

      if (!requiredMemberFields || requiredMemberFields.length === 0) {
        throw new Error(`Please input required member fields`);
      }

      await R.map((requiredMemberFields) => {
        if (!requiredMemberFields.realName) {
          throw new Error(`Missing real name for required member fields`);
        }

        if (!requiredMemberFields.email) {
          throw new Error(`Missing email for required member fields`);
        }

        if (!requiredMemberFields.bankAccount) {
          throw new Error(`Missing bank account for required member fields`);
        }

        return requiredMemberFields;
      })(requiredMemberFields);
    }

    return {
      name,
      template,
      title,
      description,
      status,
      minimumBalance,
      requiredMemberFields,
    };
  };
};

export default entity;