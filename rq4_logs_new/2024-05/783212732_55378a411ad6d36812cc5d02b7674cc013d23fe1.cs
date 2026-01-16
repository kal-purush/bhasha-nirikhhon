using Engineers;

string[] responsibilitiesSenior = new[] { "programming", "review", "testing" };
string[] responsibilitiesJuniorMiddle = new[] { "programming", "testing"};

Engineer junior = new Engineer(EngineerLevel.Junior, responsibilitiesJuniorMiddle);

// Выглядит костыльно, так ли это стоило реализовывать?
Guid someGuid = Guid.NewGuid();
Engineer junior2 = junior with { Id = someGuid };

someGuid = Guid.NewGuid();
Engineer middle = junior with { Id = someGuid, Level = EngineerLevel.Middle};

someGuid = Guid.NewGuid();
Engineer middle1 = middle with { Id = someGuid };

someGuid = Guid.NewGuid();
Engineer senior = middle with { Id = someGuid, Level = EngineerLevel.Senior, Responsibilities = responsibilitiesSenior };

someGuid = Guid.NewGuid();
Engineer senior1 = senior with { Id = someGuid};