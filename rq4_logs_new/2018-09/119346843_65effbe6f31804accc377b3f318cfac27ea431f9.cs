using GameServer.Core;
using GameServer.Core.ScriptSystem;
using GameUtil;
using System;
using System.Collections.Generic;

namespace GameServer.CharacterSystem {
	public sealed class Skill : IJSContextProvider, IDescribable, ICloneable {
		#region Javascript API class
		private sealed class JSAPI : IJSAPI<Skill> {
			private readonly Skill _outer;

			public JSAPI(Skill outer) {
				_outer = outer;
			}

			public string getName() {
				try {
					return _outer.Name;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
					return null;
				}
			}

			public void setName(string val) {
				try {
					_outer.Name = val;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
				}
			}

			public IJSAPI<SkillType> getSkillType() {
				try {
					return (IJSAPI<SkillType>)_outer.SkillType.GetContext();
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
					return null;
				}
			}

			public int getLevel() {
				try {
					return _outer.Level;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
					return 0;
				}
			}

			public void setLevel(int val) {
				try {
					_outer.Level = val;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
				}
			}

			public SkillBattleMapProperty getBattleMapProperty() {
				try {
					return _outer.BattleMapProperty;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
					return new SkillBattleMapProperty();
				}
			}

			public void setBattleMapProperty(SkillBattleMapProperty val) {
				try {
					_outer.BattleMapProperty = val;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
				}
			}

			public SkillSituationLimit getSituationLimit() {
				try {
					return _outer.SituationLimit;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
					return new SkillSituationLimit();
				}
			}

			public void setSituationLimit(SkillSituationLimit val) {
				try {
					_outer.SituationLimit = val;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
				}
			}

			public IJSAPI<Skill> clone() {
				try {
					return (IJSAPI<Skill>)_outer.Clone().GetContext();
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
					return null;
				}
			}

			public Skill Origin(JSContextHelper proof) {
				try {
					if (proof == JSContextHelper.Instance) {
						return _outer;
					}
					return null;
				} catch (Exception) {
					return null;
				}
			}
		}
		#endregion
		private readonly JSAPI _apiObj;

		private readonly SkillType _skillType;
		private string _name;
		private bool _nameUseRef = true;
		private int _level;
		private bool _levelUseRef = true;
		private SkillBattleMapProperty _battleMapProperty;
		private bool _skillPropertyUseRef = true;
		private SkillSituationLimit _situationLimit;
		private bool _situationLimitUseRef = true;

		public string Name { get => _nameUseRef ? _skillType.Name : _name; set { _name = value; _nameUseRef = false; } }
		public string Description { get => ""; set { } }
		public SkillType SkillType => _skillType;
		public int Level { get => _levelUseRef ? _skillType.Level : _level; set { _level = value; _levelUseRef = false; } }
		public SkillBattleMapProperty BattleMapProperty { get => _skillPropertyUseRef ? _skillType.BattleMapProperty : _battleMapProperty; set { _battleMapProperty = value; _skillPropertyUseRef = false; } }
		public SkillSituationLimit SituationLimit { get => _situationLimitUseRef ? _skillType.SituationLimit : _situationLimit; set { _situationLimit = value; _situationLimitUseRef = false; } }

		public Skill(SkillType skillType) {
			_skillType = skillType ?? throw new ArgumentNullException(nameof(skillType));
			_name = skillType.Name;
			_level = skillType.Level;
			_battleMapProperty = skillType.BattleMapProperty;
			_situationLimit = skillType.SituationLimit;
			_apiObj = new JSAPI(this);
		}

		public Skill Clone() {
			var ret = new Skill(_skillType);
			ret._name = _name;
			ret._nameUseRef = _nameUseRef;
			ret._level = _level;
			ret._levelUseRef = _levelUseRef;
			ret._battleMapProperty = _battleMapProperty;
			ret._skillPropertyUseRef = _skillPropertyUseRef;
			ret._situationLimit = _situationLimit;
			ret._situationLimitUseRef = _situationLimitUseRef;
			return ret;
		}

		public IJSContext GetContext() {
			return _apiObj;
		}

		public void SetContext(IJSContext context) { }

		object ICloneable.Clone() {
			return Clone();
		}
	}

	public sealed class SkillType : IJSContextProvider, IEquatable<SkillType> {
		#region Javascript API class
		private sealed class JSAPI : IJSAPI<SkillType> {
			private readonly SkillType _outer;

			public JSAPI(SkillType outer) {
				_outer = outer;
			}

			public string getID() {
				try {
					return _outer.ID;
				} catch (Exception e) {
					JSEngineManager.Engine.Log(e.Message);
					return null;
				}
			}

			public SkillType Origin(JSContextHelper proof) {
				try {
					if (proof == JSContextHelper.Instance) {
						return _outer;
					}
					return null;
				} catch (Exception) {
					return null;
				}
			}
		}
		#endregion
		private readonly JSAPI _apiObj;

		public static readonly SkillType Athletics = new SkillType("Athletics", "运动");
		public static readonly SkillType Burglary = new SkillType("Burglary", "盗窃");
		public static readonly SkillType Contacts = new SkillType("Contacts", "人脉");
		public static readonly SkillType Crafts = new SkillType("Crafts", "工艺");
		public static readonly SkillType Deceive = new SkillType("Deceive", "欺诈");
		public static readonly SkillType Drive = new SkillType("Drive", "驾驶");
		public static readonly SkillType Empathy = new SkillType("Empathy", "共情");
		public static readonly SkillType Fight = new SkillType("Fight", "战斗");
		public static readonly SkillType Investigate = new SkillType("Investigate", "调查");
		public static readonly SkillType Lore = new SkillType("Lore", "学识");
		public static readonly SkillType Notice = new SkillType("Notice", "洞察");
		public static readonly SkillType Physique = new SkillType("Physique", "体格");
		public static readonly SkillType Provoke = new SkillType("Provoke", "威胁");
		public static readonly SkillType Rapport = new SkillType("Rapport", "交际");
		public static readonly SkillType Resources = new SkillType("Resources", "资源");
		public static readonly SkillType Shoot = new SkillType("Shoot", "射击");
		public static readonly SkillType Stealth = new SkillType("Stealth", "潜行");
		public static readonly SkillType Will = new SkillType("Will", "意志");

		private static readonly Dictionary<string, SkillType> skillTypes = new Dictionary<string, SkillType>();
		public static Dictionary<string, SkillType> SkillTypes => skillTypes;

		static SkillType() {
			Athletics._situationLimit.canDefend = true;
			Athletics._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Fight._situationLimit.canAttack = true;
			Fight._situationLimit.canDefend = true;
			Fight._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Physique._situationLimit.canDefend = true;
			Physique._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Provoke._situationLimit.canAttack = true;
			Provoke._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 8, highOpen = false };
			Will._situationLimit.canDefend = true;
			Will._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Lore._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 8, highOpen = false };
			Burglary._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Contacts._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 8, highOpen = false };
			Crafts._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Deceive._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 8, highOpen = false };
			Drive._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Empathy._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 8, highOpen = false };
			Investigate._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Notice._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 8, highOpen = false };
			Rapport._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 8, highOpen = false };
			Resources._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };
			Shoot._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 8, highOpen = false };
			Shoot._battleMapProperty.islinearUse = true;
			Stealth._battleMapProperty.useRange = new Range() { low = 0, lowOpen = false, high = 1, highOpen = false };

			skillTypes.Add(Athletics.ID, Athletics);
			skillTypes.Add(Burglary.ID, Burglary);
			skillTypes.Add(Contacts.ID, Contacts);
			skillTypes.Add(Crafts.ID, Crafts);
			skillTypes.Add(Deceive.ID, Deceive);
			skillTypes.Add(Drive.ID, Drive);
			skillTypes.Add(Empathy.ID, Empathy);
			skillTypes.Add(Fight.ID, Fight);
			skillTypes.Add(Investigate.ID, Investigate);
			skillTypes.Add(Lore.ID, Lore);
			skillTypes.Add(Notice.ID, Notice);
			skillTypes.Add(Physique.ID, Physique);
			skillTypes.Add(Provoke.ID, Provoke);
			skillTypes.Add(Rapport.ID, Rapport);
			skillTypes.Add(Resources.ID, Resources);
			skillTypes.Add(Shoot.ID, Shoot);
			skillTypes.Add(Stealth.ID, Stealth);
			skillTypes.Add(Will.ID, Will);
		}

		private readonly string _id;
		private readonly string _name;
		private int _level = 0;
		private SkillBattleMapProperty _battleMapProperty = SkillBattleMapProperty.INIT;
		private SkillSituationLimit _situationLimit = new SkillSituationLimit() { canAttack = false, canDefend = false, damageMental = false };

		public string ID => _id;
		public string Name => _name;
		public int Level => _level;
		public SkillBattleMapProperty BattleMapProperty => _battleMapProperty;
		public SkillSituationLimit SituationLimit => _situationLimit;
		
		private SkillType(string id, string name) {
			_id = id ?? throw new ArgumentNullException(nameof(id));
			_name = name ?? throw new ArgumentNullException(nameof(name));
			_apiObj = new JSAPI(this);
		}

		public bool Equals(SkillType other) {
			return !(other is null) && _id == other._id;
		}

		public override bool Equals(object obj) {
			return Equals(obj as SkillType);
		}

		public override int GetHashCode() {
			return _id.GetHashCode();
		}

		public static bool operator ==(SkillType a, SkillType b) {
			return a.Equals(b);
		}

		public static bool operator !=(SkillType a, SkillType b) {
			return !(a == b);
		}

		public IJSContext GetContext() {
			return _apiObj;
		}

		public void SetContext(IJSContext context) { }
	}
}