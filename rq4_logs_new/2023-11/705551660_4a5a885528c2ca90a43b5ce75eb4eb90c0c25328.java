package com.amplifyframework.datastore.generated.model;

import com.amplifyframework.core.model.temporal.Temporal;
import com.amplifyframework.core.model.annotations.HasMany;
import com.amplifyframework.core.model.ModelIdentifier;

import java.util.List;
import java.util.UUID;
import java.util.Objects;

import androidx.core.util.ObjectsCompat;

import com.amplifyframework.core.model.AuthStrategy;
import com.amplifyframework.core.model.Model;
import com.amplifyframework.core.model.ModelOperation;
import com.amplifyframework.core.model.annotations.AuthRule;
import com.amplifyframework.core.model.annotations.Index;
import com.amplifyframework.core.model.annotations.ModelConfig;
import com.amplifyframework.core.model.annotations.ModelField;
import com.amplifyframework.core.model.query.predicate.QueryField;

import static com.amplifyframework.core.model.query.predicate.QueryField.field;

/** This is an auto generated class representing the Team type in your schema. */
@SuppressWarnings("all")
@ModelConfig(pluralName = "Teams", type = Model.Type.USER, version = 1, authRules = {
  @AuthRule(allow = AuthStrategy.PUBLIC, operations = { ModelOperation.CREATE, ModelOperation.UPDATE, ModelOperation.DELETE, ModelOperation.READ })
})
public final class Team implements Model {
  public static final QueryField ID = field("Team", "id");
  public static final QueryField TEAM_NAME = field("Team", "team_name");
  public static final QueryField DATE_CREATED = field("Team", "dateCreated");
  private final @ModelField(targetType="ID", isRequired = true) String id;
  private final @ModelField(targetType="String", isRequired = true) String team_name;
  private final @ModelField(targetType="AWSDateTime") Temporal.DateTime dateCreated;
  private final @ModelField(targetType="Task") @HasMany(associatedWith = "teamTask", type = Task.class) List<Task> tasks = null;
  private @ModelField(targetType="AWSDateTime", isReadOnly = true) Temporal.DateTime createdAt;
  private @ModelField(targetType="AWSDateTime", isReadOnly = true) Temporal.DateTime updatedAt;
  /** @deprecated This API is internal to Amplify and should not be used. */
  @Deprecated
   public String resolveIdentifier() {
    return id;
  }
  
  public String getId() {
      return id;
  }
  
  public String getTeamName() {
      return team_name;
  }
  
  public Temporal.DateTime getDateCreated() {
      return dateCreated;
  }
  
  public List<Task> getTasks() {
      return tasks;
  }
  
  public Temporal.DateTime getCreatedAt() {
      return createdAt;
  }
  
  public Temporal.DateTime getUpdatedAt() {
      return updatedAt;
  }
  
  private Team(String id, String team_name, Temporal.DateTime dateCreated) {
    this.id = id;
    this.team_name = team_name;
    this.dateCreated = dateCreated;
  }
  
  @Override
   public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if(obj == null || getClass() != obj.getClass()) {
        return false;
      } else {
      Team team = (Team) obj;
      return ObjectsCompat.equals(getId(), team.getId()) &&
              ObjectsCompat.equals(getTeamName(), team.getTeamName()) &&
              ObjectsCompat.equals(getDateCreated(), team.getDateCreated()) &&
              ObjectsCompat.equals(getCreatedAt(), team.getCreatedAt()) &&
              ObjectsCompat.equals(getUpdatedAt(), team.getUpdatedAt());
      }
  }
  
  @Override
   public int hashCode() {
    return new StringBuilder()
      .append(getId())
      .append(getTeamName())
      .append(getDateCreated())
      .append(getCreatedAt())
      .append(getUpdatedAt())
      .toString()
      .hashCode();
  }
  
  @Override
   public String toString() {
    return new StringBuilder()
      .append("Team {")
      .append("id=" + String.valueOf(getId()) + ", ")
      .append("team_name=" + String.valueOf(getTeamName()) + ", ")
      .append("dateCreated=" + String.valueOf(getDateCreated()) + ", ")
      .append("createdAt=" + String.valueOf(getCreatedAt()) + ", ")
      .append("updatedAt=" + String.valueOf(getUpdatedAt()))
      .append("}")
      .toString();
  }
  
  public static TeamNameStep builder() {
      return new Builder();
  }
  
  /**
   * WARNING: This method should not be used to build an instance of this object for a CREATE mutation.
   * This is a convenience method to return an instance of the object with only its ID populated
   * to be used in the context of a parameter in a delete mutation or referencing a foreign key
   * in a relationship.
   * @param id the id of the existing item this instance will represent
   * @return an instance of this model with only ID populated
   */
  public static Team justId(String id) {
    return new Team(
      id,
      null,
      null
    );
  }
  
  public CopyOfBuilder copyOfBuilder() {
    return new CopyOfBuilder(id,
      team_name,
      dateCreated);
  }
  public interface TeamNameStep {
    BuildStep teamName(String teamName);
  }
  

  public interface BuildStep {
    Team build();
    BuildStep id(String id);
    BuildStep dateCreated(Temporal.DateTime dateCreated);
  }
  

  public static class Builder implements TeamNameStep, BuildStep {
    private String id;
    private String team_name;
    private Temporal.DateTime dateCreated;
    public Builder() {
      
    }
    
    private Builder(String id, String team_name, Temporal.DateTime dateCreated) {
      this.id = id;
      this.team_name = team_name;
      this.dateCreated = dateCreated;
    }
    
    @Override
     public Team build() {
        String id = this.id != null ? this.id : UUID.randomUUID().toString();
        
        return new Team(
          id,
          team_name,
          dateCreated);
    }
    
    @Override
     public BuildStep teamName(String teamName) {
        Objects.requireNonNull(teamName);
        this.team_name = teamName;
        return this;
    }
    
    @Override
     public BuildStep dateCreated(Temporal.DateTime dateCreated) {
        this.dateCreated = dateCreated;
        return this;
    }
    
    /**
     * @param id id
     * @return Current Builder instance, for fluent method chaining
     */
    public BuildStep id(String id) {
        this.id = id;
        return this;
    }
  }
  

  public final class CopyOfBuilder extends Builder {
    private CopyOfBuilder(String id, String teamName, Temporal.DateTime dateCreated) {
      super(id, team_name, dateCreated);
      Objects.requireNonNull(team_name);
    }
    
    @Override
     public CopyOfBuilder teamName(String teamName) {
      return (CopyOfBuilder) super.teamName(teamName);
    }
    
    @Override
     public CopyOfBuilder dateCreated(Temporal.DateTime dateCreated) {
      return (CopyOfBuilder) super.dateCreated(dateCreated);
    }
  }
  

  public static class TeamIdentifier extends ModelIdentifier<Team> {
    private static final long serialVersionUID = 1L;
    public TeamIdentifier(String id) {
      super(id);
    }
  }
  
}