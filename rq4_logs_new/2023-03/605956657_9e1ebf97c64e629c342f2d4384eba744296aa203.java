package pro.sky.recipessite.controllers;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.media.Content;

@OpenAPIDefinition(
        info = @Info(
                title = "Cайт рецептов \"Рецептурная\"",
                description = "API сайта рецептов",
                version = "1.0.0.",
                contact = @Contact(
                        name = "Сироткина Яна Евгеньевна",
                        email = "a421243266@gmail.com",
                        url = "https://github.com/Spolpinka/RecipesSite"
                )
        )
)
public class OpenApiConfig {
}