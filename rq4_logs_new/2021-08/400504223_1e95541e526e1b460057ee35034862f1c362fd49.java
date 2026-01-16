package by.intro.client.model;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Account {

    private String login;
    @EqualsAndHashCode.Exclude
    private String password;
    private AccountRole role;
}