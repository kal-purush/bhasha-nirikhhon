package com.raisetech.inventoryapi.form;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class UpdateInventoryProductFormTest {
    private static Validator validator;

    @BeforeAll
    public static void setUpValidator() {
        Locale.setDefault(Locale.JAPAN);
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    public void 数量がゼロのときにバリデーションエラーとなること() {
        int quantity = 0;
        UpdateInventoryProductForm updateInventoryProductForm = new UpdateInventoryProductForm(1, quantity);
        Set<ConstraintViolation<UpdateInventoryProductForm>> violations = validator.validate(updateInventoryProductForm);
        assertThat(violations).hasSize(1);
        assertThat(violations)
                .extracting(violation -> violation.getPropertyPath().toString(), ConstraintViolation::getMessage)
                .containsExactlyInAnyOrder(tuple("quantity", "1 以上の値にしてください"));
    }

    @Test
    public void 数量がマイナスのときにバリデーションエラーとなること() {
        int quantity = -500;
        UpdateInventoryProductForm updateInventoryProductForm = new UpdateInventoryProductForm(1, quantity);
        Set<ConstraintViolation<UpdateInventoryProductForm>> violations = validator.validate(updateInventoryProductForm);
        assertThat(violations).hasSize(1);
        assertThat(violations)
                .extracting(violation -> violation.getPropertyPath().toString(), ConstraintViolation::getMessage)
                .containsExactlyInAnyOrder(tuple("quantity", "1 以上の値にしてください"));
    }

    @Test
    public void 数量がnullのときにバリデーションエラーとなること() {
        Integer quantity = null;
        UpdateInventoryProductForm updateInventoryProductForm = new UpdateInventoryProductForm(1, quantity);
        Set<ConstraintViolation<UpdateInventoryProductForm>> violations = validator.validate(updateInventoryProductForm);
        assertThat(violations).hasSize(1);
        assertThat(violations)
                .extracting(violation -> violation.getPropertyPath().toString(), ConstraintViolation::getMessage)
                .containsExactlyInAnyOrder(tuple("quantity", "null は許可されていません"));
    }

    @Test
    public void 数量が正の整数のときにバリデーションエラーとならないこと() {
        int quantity = 500;
        UpdateInventoryProductForm updateInventoryProductForm = new UpdateInventoryProductForm(1, quantity);
        Set<ConstraintViolation<UpdateInventoryProductForm>> violations = validator.validate(updateInventoryProductForm);
        assertThat(violations).isEmpty();
    }
}