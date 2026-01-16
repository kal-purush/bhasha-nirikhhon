// Copyright 2020 Alkaline Games, LLC. All Rights Reserved.

using UnrealBuildTool;

public class AlkModAddit : ModuleRules
{
  public AlkModAddit(ReadOnlyTargetRules Target) : base(Target)
  {
    PCHUsage = PCHUsageMode.UseExplicitOrSharedPCHs;
    PrivateDependencyModuleNames.AddRange(new string[] {
      "Core",
      "CoreUObject",
      "Engine"
    });
  }
}