using R2API;
using BepInEx;
using RoR2;
using RoR2.Skills;
using R2API.Utils;
using UnityEngine;
using UnityEngine.AddressableAssets;

namespace CustomRoRMod {

	[BepInPlugin("com.Noodle.Mod", "Mod", "1.0.0")]
	[BepInDependency(R2API.R2API.PluginGUID)]
	[R2APISubmoduleDependency(
		nameof(ItemAPI),
		nameof(LanguageAPI),
		nameof(RecalculateStatsAPI),
		nameof(ContentAddition)
	)]
	[NetworkCompatibility(CompatibilityLevel.EveryoneMustHaveMod,VersionStrictness.EveryoneNeedSameModVersion)]
	public class Mod : BaseUnityPlugin {

		BuffDef momentum;
		SkillDef skill;
		UnlockableDef unlockable;

		readonly float maxStacks = 10;
		readonly float timerDuration = 1;

		float buffTimer = 1;
		bool clearingBuff = false;

		CharacterBody player = null;

		private void Awake() {

			// Create Momentum Buff Definition
			momentum = ScriptableObject.CreateInstance<BuffDef>();

			momentum.name = "NOODLE_HUNTRESSBUFF_NAME";
			momentum.canStack = true;
			momentum.isDebuff = false;
			momentum.isHidden = false;
			momentum.iconSprite = Addressables.LoadAssetAsync<Sprite>("RoR2/Base/UI/texBasicArrowRight.png").WaitForCompletion();

			ContentAddition.AddBuffDef(momentum);

			// Create Momentum Skill Definition
			skill = ScriptableObject.CreateInstance<SkillDef>();

			(skill as ScriptableObject).name = "Momentum";
			skill.icon = Addressables.LoadAssetAsync<Sprite>("RoR2/Base/UI/texBasicArrowRight.png").WaitForCompletion();
			skill.activationState = new EntityStates.SerializableEntityStateType("EntityStates.Idle");
			skill.activationStateMachineName = "Body";
			skill.baseMaxStock = 1;
			skill.baseRechargeInterval = 0;
			skill.beginSkillCooldownOnSkillEnd = false;
			skill.isCombatSkill = false;
			skill.mustKeyPress = false;
			skill.skillNameToken = "NOODLE_HUNTRESSPASSIVE_NAME";
			skill.skillDescriptionToken = "NOODLE_HUNTRESSPASSIVE_DESC";

			LanguageAPI.Add(skill.skillNameToken, "Momentum");
			LanguageAPI.Add(skill.skillDescriptionToken, $"Sprinting gives stacks of momentum at {maxStacks} stacks the next attack will crit");

			ContentAddition.AddSkillDef(skill);

			// Create Momentum Unlockable Definition
			unlockable = ScriptableObject.CreateInstance<UnlockableDef>();
			unlockable.cachedName= "NOODLE_HUNTRESSUNLOCK_NAME";

			unlockable.getHowToUnlockString = delegate () {return "Huntress Passive Unlock"; };
			unlockable.getUnlockedString = delegate () {return "Unlocked Huntress Passive"; };
			unlockable.nameToken = "NOODLE_HUNTRESSUNLOCK_NAME";

			LanguageAPI.Add(unlockable.nameToken, "Huntress Unlock Name");

			ContentAddition.AddUnlockableDef(unlockable);

			// Create Momentum Skill Family
			SkillFamily skillFamily = ScriptableObject.CreateInstance<SkillFamily>();
			(skillFamily as ScriptableObject).name = "HuntressBodyPassiveFamily";
			skillFamily.variants = new SkillFamily.Variant[1];
			skillFamily.variants[0] = new SkillFamily.Variant {

				skillDef = skill,
				unlockableDef = unlockable
			};

			ContentAddition.AddSkillFamily(skillFamily);

			// Add Momentum Skill onto Huntress
			GameObject huntressBody = Addressables.LoadAssetAsync<GameObject>("RoR2/Base/Huntress/HuntressBody.prefab").WaitForCompletion();
			GenericSkill passiveSkill = huntressBody.AddComponent<GenericSkill>();
			passiveSkill.SetFieldValue("_skillFamily", skillFamily);



			On.RoR2.GlobalEventManager.OnHitAll += GlobalEventManager_OnHitAll;
			On.RoR2.GenericSkill.Start += GenericSkill_Start;

			RecalculateStatsAPI.GetStatCoefficients += GetStatCoefficients;
		}

		// Check if the player has the Momentum Skill
		private void GenericSkill_Start(On.RoR2.GenericSkill.orig_Start orig, GenericSkill self) {

			orig(self);
			if(self.skillDef == skill) {

				player = self.characterBody;
			}
		}

		// Gurantees Crit
		private void GetStatCoefficients(CharacterBody sender, RecalculateStatsAPI.StatHookEventArgs args) {

			if(sender) {

				// Check if the player has enough stacks
				if(sender.GetBuffCount(momentum) >= maxStacks) {

					args.critAdd = 100;
				}
			}
		}

		private void FixedUpdate() {

			if(player) {
				
				if(player.isSprinting) {

					// Recalculate crit on the player if they have enough stacks
					if(player.GetBuffCount(momentum) >= maxStacks) {

						player.RecalculateStats();
					}
					else {

						// Increments the timer
						if(buffTimer > 0) {

							buffTimer -= Time.deltaTime;
						}
						else {

							// Resets the timer and gives the player a stack
							buffTimer = timerDuration;
							player.AddBuff(momentum);
						}
					}
				}
			}
		}

		private void GlobalEventManager_OnHitAll(On.RoR2.GlobalEventManager.orig_OnHitAll orig, GlobalEventManager self, DamageInfo damageInfo, GameObject hitObject) {

			orig.Invoke(self, damageInfo, hitObject);

			CharacterBody attackerBody = damageInfo.attacker.GetComponent<CharacterBody>();
			if(attackerBody) {

				if(attackerBody.HasBuff(momentum) && damageInfo.crit && !clearingBuff) {

					ClearMomentum(attackerBody);
				}
			}
		}

		/// Removes all of Momentum stacks DONT TOUCH
		private void ClearMomentum(CharacterBody body) {

			if(body) {

				if(body.HasBuff(momentum)) {

					clearingBuff = true;
					for(int i = body.GetBuffCount(momentum); i > 0; i--) {

						body.RemoveBuff(momentum);
					}

					clearingBuff = false;
				}
			}
		}

		private void Log(string message) {

			Logger.Log(BepInEx.Logging.LogLevel.Debug, message);
		}
	}
}
