package de.raidcraft.achievements.entities;

import com.google.common.base.Strings;
import de.exlll.configlib.annotation.ConfigurationElement;
import io.ebean.Finder;
import io.ebean.annotation.DbJson;
import io.ebean.annotation.Index;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.java.Log;
import net.silthus.ebean.BaseEntity;

import javax.persistence.Entity;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static de.raidcraft.achievements.Constants.TABLE_PREFIX;

@Entity
@Getter
@Setter
@ConfigurationElement
@Accessors(fluent = true)
@Log(topic = "RCAchievements")
@Table(name = TABLE_PREFIX + "categories")
public class Category extends BaseEntity {

    public static final Finder<UUID, Category> find = new Finder<>(Category.class);

    /**
     * Tries to find the given category by its id.
     *
     * @param uuid the id of the category
     * @return the category or an empty optional
     */
    public static Optional<Category> byId(@NonNull UUID uuid) {

        return Optional.ofNullable(find.byId(uuid));
    }

    /**
     * Tries to find a category with the given alias.
     * <p>The alias can be null or empty, but then an empty optional will be returned.
     *
     * @param alias the alias of the category. can be null or empty.
     * @return the category if it exists
     */
    public static Optional<Category> byAlias(String alias) {

        if (Strings.isNullOrEmpty(alias)) return Optional.empty();

        return find.query().where()
                .ieq("alias", alias)
                .findOneOrEmpty();
    }

    /**
     * Tries to find a category with the given alias or id.
     * <p>The alias can be null or empty, but then an empty optional will be returned.
     *
     * @param alias the alias or id of the category. can be null or empty.
     * @return the category if it exists
     */
    public static Optional<Category> byAliasOrId(String alias) {

        if (Strings.isNullOrEmpty(alias)) return Optional.empty();

        try {
            return byId(UUID.fromString(alias));
        } catch (IllegalArgumentException e) {
            return byAlias(alias);
        }
    }

    /**
     * Creates a new category using the given alias.
     * <p>Will return the existing category if one exists.
     * <p>{@link Category#save()} needs to be called after setting the parameters.
     *
     * @param alias the alias of the category
     * @return a new or existing category with the alias
     */
    public static Category create(String alias) {

        return Category.byAlias(alias).orElse(new Category(alias));
    }

    @Index(unique = true)
    private String alias;
    private String name;
    @DbJson
    private List<String> description = new ArrayList<>();

    @OneToMany
    private List<Achievement> achievements = new ArrayList<>();

    Category(String alias) {

        this.alias = alias;
    }
}