using UnityEngine;
using System.Collections;
using System.Collections.Generic;


//LogisticsHoppers should have no vacuum, no filter orders (maybe?) and only super super slow attaching to conveyors

//Do not current require power
public class StorageHopper : MachineEntity, PowerConsumerInterface, ItemConsumerInterface, StorageMachineInterface
{
	//private bool stringDirty;
    public static ushort STORAGE_HOPPER = 0;
	public static ushort CRYO_HOPPER = 3;
	public static ushort MOTORISED_HOPPER = 4;
	public static float SAFE_COLD_TEMP = -250;//yeah, so absolute zero innit
	
	public static int PowerPerItem = 5;//theoretical max of 150/face, which is 750/min per face.
	
	public ItemBase[] maItemInventory; // only items now, no more cube ducttape.
	
	public ushort[] maStorage;//outdated - do not use!
	float mrMaxPower = 1000.0f;//this is a lot, more than the suit can carry in one go!
	public float mrCurrentPower = 0.0f;
	public float mrCurrentTemperature;
	
	int mnMaxStorage = 100;//start smaller and upgrade?
	public int mnStorageUsed = 0;//needs to be calculated
	public int mnStorageFree = 0;

	public float mrNormalisedPower;

	long ExtractionX;
	long ExtractionY;
	long ExtractionZ;
		
	public float mrExtractionTime = 15.0f;//seconds/ore
	
	public float mrPowerUsage = 0.1f;//power/sec
	
//	public GameObject MiningSparks;
	
	bool mbLinkedToGO;
	Light WorkLight; 
	TextMesh mTextMesh;
	ParticleSystem mHooverPart;
	GameObject mHopperPart;

	GameObject mHoloStatus;
		
	public string mLastItemAdded;
	
	public float mrReadoutTick;
	int mnLowFrequencyUpdates;
	int mnReadouts;
	
	public eHopperPermissions mPermissions = eHopperPermissions.AddAndRemove;
	public bool mbHooverOn;

	public bool mbAllowLogistics;//This flickers high/low for low tiers
	float mrLogisticsDebounce;
	float mrTimeSinceLogistics;

	static int[] mRemoveCache = new int[100];

	public void LogisticsOperation()
	{
		mrTimeSinceLogistics =	mrLogisticsDebounce;
		if(mrTimeSinceLogistics > 0) mbAllowLogistics = false;

	}
	
	// ***************************************************************************************************************************************
	public StorageHopper(Segment segment, long x, long y, long z, ushort cube, byte flags, ushort lValue, bool lbFromDisk) : base(eSegmentEntity.StorageHopper, SpawnableObjectEnum.StorageHopper, x, y, z, cube, flags, lValue, Vector3.zero, segment)
	{
		mbNeedsLowFrequencyUpdate = true;
		mbNeedsUnityUpdate = true;
		
//		maStorage = new ushort[mnMaxStorage];
		
//		for (int i=0;i<mnMaxStorage;i++)
//		{
//			maStorage[i] = eCubeTypes.NULL;
//		}
		
		//Hoppers will hold 100 items in total, which is 100 differnt items, or 100 of a single item. 
		//So we need to store the worst fucking case ¬.¬
		maItemInventory = new ItemBase[mnMaxStorage];

		mrMaxPower = 0;

		if (lValue == MOTORISED_HOPPER)
		{
			mnMaxStorage = 2;//
			mrLogisticsDebounce = 0;//Fast!
			mrMaxPower = 256;//dunno
		}

		if (lValue == CRYO_HOPPER)//CH
		{
			mnMaxStorage = 50;//This should allow 1 CH and a 50:1 of pristine -> organics
			mrLogisticsDebounce = 0;//Fast!
			mrMaxPower = 500;//ouch!
		} 

		if (lValue == 2)//LH
		{
			mnMaxStorage = 2;
			mrLogisticsDebounce = 2;//Logistics Hopper is fast, but has no storage
			if (DifficultySettings.mbRushMode) mrLogisticsDebounce = 1;
		} 
		if (lValue == 1)//MH
		{
			mnMaxStorage = 10;
			mrLogisticsDebounce = 30.0f;//Microhopper
			//raised from 10; you can easily get 4 of these around a single OE and mitigate having proper SHs for ages and ages.
			if (DifficultySettings.mbRushMode) mrLogisticsDebounce = 3;
		}
	
		if (lValue == 0)
		{
			mrLogisticsDebounce = 0;
			mnMaxStorage = 100;
		}

		CountFreeSlots();

		CheckSegments = new Segment[6];

		//I haven't really got a good way to do this...

		if (SteamManager.Initialized && !lbFromDisk)
		{
			if (SurvivalPlayerScript.meTutorialState < SurvivalPlayerScript.eTutorialState.NowFuckOff)
			{
				AddCube(eCubeTypes.OreCoal,0); // WRONG VALUE.
				AddCube(eCubeTypes.OreCoal,0);
				AddCube(eCubeTypes.OreCoal,0);
				AddCube(eCubeTypes.OreCoal,0);
				AddCube(eCubeTypes.OreCoal,0);
				Debug.LogWarning("Finished adding free coal!");
			}
		}
	}
	public override void SpawnGameObject()
	{
		// object based on value for tiers
		//	Debug.Log("LPT spawning with Value of " + mValue);
		if (mValue == CRYO_HOPPER)
		{
			mObjectType = SpawnableObjectEnum.CryoHopper;
		}

		if (mValue == MOTORISED_HOPPER)
		{
			mObjectType = SpawnableObjectEnum.MotorisedLogisticsHopper;
		}


		if (mValue == 2)
		{
			mObjectType = SpawnableObjectEnum.LogisticsHopper;
		}
		//Old LH becomes new MH (ELSE SHIT ON DISK IS FUCKED AS THE STORAGE IS TOO SMALL)
		if (mValue == 1)
		{
			mObjectType = SpawnableObjectEnum.MicroHopper;
		}
		
        if (mValue == STORAGE_HOPPER)
		{
			mObjectType = SpawnableObjectEnum.StorageHopper;
		}
		base.SpawnGameObject();
	}	
	// ***************************************************************************************************************************************	
	public override int GetVersion()
	{
		return 1;
	}

	// ***************************************************************************************************************************************	
	public override void DropGameObject ()
	{
		base.DropGameObject ();
		mbLinkedToGO = false;
        //mHoloMPB = null;
		GameObject.Destroy(TutorialEffect);
	}
	GameObject TutorialEffect;
	
	// ***************************************************************************************************************************************	
	public bool AddItem(ItemBase lItemToAdd)
	{
		//It's valid to ignore this if it's a conveyor
		//if (mPermissions == ePermissions.RemoveOnly) Debug.LogWarning("Warning, about to add " + ItemManager.GetItemName(lItemToAdd.mnItemID) +" to a Remove Only hopper!");
		//if (mPermissions == ePermissions.Locked) 	Debug.LogWarning("Warning, about to add " + ItemManager.GetItemName(lItemToAdd.mnItemID) +" to a Locked hopper!");
		if (lItemToAdd == null)
		{
			//This should really never happen (it happens only as a network client, it seems)
#if UNITY_EDITOR
			Debug.LogError("Warning, Hopper had null item added - oops?" + System.Environment.StackTrace);
#endif
			return true; // can always fit nothing, lal
		}
		
		CountFreeSlots(); // this shouldn't be necessary, but okay.

		if (mnStorageFree <= 0)
			return false;
		
		if (ItemManager.GetCurrentStackSize(lItemToAdd) > mnStorageFree)
			return false;

		if (lItemToAdd.mType == ItemType.ItemStack)
			if ((lItemToAdd as ItemStack).mnAmount == 0)
			{
				Debug.LogError("Error, attempting to add an ItemStack of ZERO to the SH?![" + ItemManager.GetItemName(lItemToAdd) +"]");
				return false;
			}

        #if UNITY_EDITOR
        if (lItemToAdd.mType == ItemType.ItemCubeStack)
            if (CubeHelper.IsIngottableOre((lItemToAdd as ItemCubeStack).mCubeType))
            {
                if((lItemToAdd as ItemCubeStack).mCubeValue == 0)
                {
                    Debug.LogError("Error, SH got ore with no value?" + System.Environment.StackTrace);
                }
            }
        #endif
		
		//Do we attempt to collapse stacks at this point? yes, saves processing power when checking inventories, which happens a lot!
		for (int i = 0; i < mnMaxStorage; i++)
		{
			ItemBase existingItem = maItemInventory[i];
			
			if (existingItem == null)
				continue;
			
			if (existingItem.mType != lItemToAdd.mType) // this also covers cube stacks :)
				continue;
			
			if (ItemManager.StackWholeItems(existingItem, lItemToAdd, false))
			{
				CountFreeSlots();
				MarkDirtyDelayed();

				if (WorldScript.mLocalPlayer.mResearch.IsKnown(lItemToAdd))
				{
					mLastItemAdded = ItemManager.GetItemName(lItemToAdd);
				}
				else
				{
					mLastItemAdded = PlayerResearch.UnknownMaterial;
				}

				return true;				
			}
		}
		
		// couldn't collapse, add to new spot
		for (int i = 0; i < mnMaxStorage; i++)
		{
			if (maItemInventory[i] == null)	
			{
				maItemInventory[i] = lItemToAdd;

				if (WorldScript.mLocalPlayer.mResearch.IsKnown(lItemToAdd))
				{
					mLastItemAdded = ItemManager.GetItemName(lItemToAdd);
				}
				else
				{
					mLastItemAdded = PlayerResearch.UnknownMaterial;
				}

				CountFreeSlots();
				MarkDirtyDelayed();
				
				return true;
			}
		}
		//Debug.Log("Failed to add item to Storage Hopper!");
		return false;
	}
	
	// ***************************************************************************************************************************************	
	void CountFreeSlots()
	{
		int lnOldStorage = mnStorageFree;
		mnStorageUsed = 0;
		//Are hoppers limited to 100? If so, we need to also do this
		
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] != null)
			{
				ItemBase item = maItemInventory[i];
				
				if (item.mType == ItemType.ItemStack)
					mnStorageUsed += (item as ItemStack).mnAmount;
				else if (item.mType == ItemType.ItemCubeStack)
					mnStorageUsed += (item as ItemCubeStack).mnAmount;
				else
					mnStorageUsed++;
			}
		}
		if (mnStorageUsed > mnMaxStorage)
		{
			//Ore Extractors have been putting more than they should into a hopper, making the stack size > max!
            if (WorldScript.mbIsServer)
            {
                ServerConsole.DebugLog("Storage hopper has overflowed! " + mnStorageUsed +"/" + mnMaxStorage +"." + System.Environment.StackTrace + "Last Item was " + mLastItemAdded, ConsoleMessageType.Error);
			    Debug.LogError("Storage hopper has overflowed! " + mnStorageUsed +"/" + mnMaxStorage +"." + System.Environment.StackTrace + "Last Item was " + mLastItemAdded);
            }
		}
		
		
		mnStorageFree = mnMaxStorage - mnStorageUsed;
		if (lnOldStorage != mnStorageFree) mbForceTextUpdate = true;
	
	}
	bool mbForceTextUpdate;
	bool mbForceHoloUpdate;

	Segment[] CheckSegments;
 	// *******************************************************************W********************************************************************	
	float mrSleep;//If have no machines attached to us, we can sleep for a second or so
	float mrTimeUntilPlayerDistanceUpdate;
	public override void LowFrequencyUpdate()
	{

		if (mrTimeSinceLogistics > 0.0f)
		{
			mrTimeSinceLogistics -= LowFrequencyThread.mrPreviousUpdateTimeStep;
			mbAllowLogistics = false;
		}
		else
		{
			mbAllowLogistics = true;
		}

		//This makes a shockingly small amount of difference!



		mrTimeUntilPlayerDistanceUpdate -= LowFrequencyThread.mrPreviousUpdateTimeStep;
		if (mrTimeUntilPlayerDistanceUpdate < 0)
		{
            mrPrevDistanceToPlayer = mDistanceToPlayer;
			UpdatePlayerDistanceInfo();
			mrTimeUntilPlayerDistanceUpdate = mDistanceToPlayer / 30;//1 second at 30 metres, .5 at 15 metres (halved from before)
			if (mrTimeUntilPlayerDistanceUpdate > 2) mrTimeUntilPlayerDistanceUpdate = 2;
		}

		mnLowFrequencyUpdates++;
		
		UpdateHoover();

		UpdatePoweredHopper();//Keep the client sim in step

		if (WorldScript.mbIsServer == true)//this is done by the server
		{
		    UpdateSpoilage();
		}

		/*mrCurrentPower += mrIsotopeRate * GameManager.mrPreviousUpdateTimeStep;
		mrCurrentPower += mrSolarEfficiency * GameManager.mrPreviousUpdateTimeStep * Mathf.Abs(SurvivalWeatherManager.mrSunAngle);
		if(mrCurrentPower > mrMaxPower) mrCurrentPower = mrMaxPower;*/

		mrNormalisedPower = mrCurrentPower / mrMaxPower;
		
		mrReadoutTick -= LowFrequencyThread.mrPreviousUpdateTimeStep;
	//	Debug.Log(mrReadoutTick+"::" + GameManager.mrPreviousUpdateTimeStep.ToString("F2"));
		if (mrReadoutTick < 0.0f)
		{
			
		//In museum mode, we continually empty out slots, just in case;this is hooked up to the 1 second readout tick 
		//Which is actually the tick where we look for places to hand out stuff. 
#if MUSEUM		
	//	public ItemBase[] maItemInventory;
		maItemInventory[0] = null;
		maStorage[99] = eCubeTypes.NULL;//this allows it to fill up first
		CountFreeSlots();
#endif
		
			
			mrReadoutTick = 1.0f;
			mnReadouts++;
			
			//UnityEngine.Debug.Log("SH checking on update "+ mnReadouts + " with " + mnStorageUsed + " storage used");
			//See if anything adjacent wants some of our resources
		//	if (mnStorageUsed > 0)//removed as we both give and take resources
			{

				long checkX = this.mnX;
				long checkY = this.mnY;
				long checkZ = this.mnZ;

				int lnWhich = mnReadouts % 6;
				
				if (lnWhich == 0) checkX--;
				if (lnWhich == 1) checkX++;
				if (lnWhich == 2) checkY--;
				if (lnWhich == 3) checkY++;
				if (lnWhich == 4) checkZ--;
				if (lnWhich == 5) checkZ++;

				if (CheckSegments[lnWhich] == null)
				{
					CheckSegments[lnWhich] = AttemptGetSegment(checkX, checkY, checkZ);
					return;
				}

				if (CheckSegments[lnWhich].mbDestroyed || CheckSegments[lnWhich].mbInitialGenerationComplete == false)
				{
					CheckSegments[lnWhich] = null;
					return;
				}
				

				ushort lCube = CheckSegments[lnWhich].GetCube(checkX, checkY, checkZ);

				if (CubeHelper.HasEntity(lCube) == false) return;//don't bother, it's not a machine entity
	
				// machines that give us resources
				if (mnStorageFree > 0) CheckSuppliers(CheckSegments[lnWhich], lCube, checkX, checkY, checkZ);

				// machines that take our resources
				if (mnStorageUsed > 0) CheckConsumers(CheckSegments[lnWhich], lCube, checkX, checkY, checkZ);

				//if  (mnReadouts % mnHopperLoops == 0)
				{
					//both directions
					CheckHoppers (checkX, checkY, checkZ, CheckSegments[lnWhich], lCube);
				}
			}
		}
	
		//Look for adjacent places to transfer storage
		//Maybe only do this if we have power? Maybe.

	}
	// ****************************************************************************************************
	public const ushort SPOILED_ORGANICS = 4100;
	float mrSpoilTimer = 30;
	void UpdateSpoilage()
	{
		bool lbSpoil = true;


		//The ONLY way items do not spoil is if it's a Cryo and temperature is below the SAFE temp
		if (mValue == CRYO_HOPPER && mrCurrentTemperature < SAFE_COLD_TEMP) lbSpoil = false;

		if (lbSpoil)
		{
			mrSpoilTimer -= LowFrequencyThread.mrPreviousUpdateTimeStep;

			if (mrSpoilTimer <= 0)
				{
				//Look for an item to spoil.
				for (int i=0;i<mnMaxStorage;i++)
				{
					if (AttemptToSpoilOrganicItem(i))
					{
						mrSpoilTimer = 30;
						return;//hurray! Well, boo.
					}
				}
				//Nothing spoiled!
				mrSpoilTimer = 30;
			}
		}
		else
		{
			mrSpoilTimer = 30.0f;
		}

	}
	// ****************************************************************************************************
	bool AttemptToSpoilOrganicItem(int i)
	{
		if (maItemInventory[i] == null) return false;
		
		//convert any odd numbered item from 4000 upwards to 4010
		
		if (maItemInventory[i].mnItemID >= 4000 && maItemInventory[i].mnItemID <= 4010)
		{
			if (maItemInventory[i].mnItemID % 2 == 1)
			{
				//This means it's ruined - these can spoil!
				
				mrSpoilTimer = 30.0f;
				DecrementInventorySlot(i);
				
				ItemBase lSpoiledItem = ItemManager.SpawnItem(SPOILED_ORGANICS);
				
				AddItem(lSpoiledItem);

				return true;
			}
		}

		//chilled items convert to ruined

		return false;
	}
	// ****************************************************************************************************
	public float mrPPS;
	void UpdatePoweredHopper()
	{


		if (mValue != CRYO_HOPPER) return;//todo, also worry about heater hoppers

        bool lbInColdCavern = false;

		float lrExternalTemp = 0;
		if (mnY-WorldScript.mDefaultOffset < BiomeLayer.CavernColdCeiling && mnY-WorldScript.mDefaultOffset > BiomeLayer.CavernColdFloor)
		{
			//this is now only a bit different from the cold internal temp
			lrExternalTemp = -225;//above the SAFE_COLD_TEMP, but not a long way. Cuz 50 degrees isn't a long way.
            lbInColdCavern = true;
		}

	

		//Lerp towards external temperature
		float lrDiff = lrExternalTemp - mrCurrentTemperature;
		lrDiff *= LowFrequencyThread.mrPreviousUpdateTimeStep;
		lrDiff /= 50.0f;
		mrCurrentTemperature += lrDiff;

		//Now lerp towards desired temperature
		float lrDesiredTemp = -300;
		lrDiff = lrDesiredTemp - mrCurrentTemperature;
		lrDiff *= LowFrequencyThread.mrPreviousUpdateTimeStep;
  

		float lrPowerNeeded = lrDiff;//ouch

		if (lrPowerNeeded <0) lrPowerNeeded = -lrPowerNeeded;//we don't care what direction we're changing the temperature, it never requires negative power

		if (lrPowerNeeded > mrCurrentPower)
		{
			lrPowerNeeded = mrCurrentPower;

		}

		if (lrDiff > 0) 
			lrDiff = lrPowerNeeded;
		else
			lrDiff = -lrPowerNeeded;//Negative temperature change 	

        //Cold cavern gives a bit cheaty +50% power efficiency to CryoHoppers
        if (lbInColdCavern)
        {
            lrPowerNeeded /= 2.0f;
        }

		mrCurrentPower -= lrPowerNeeded; 
		mrCurrentTemperature += lrDiff * 0.1f; //* Efficiency (inverted because power is inversion of cooling)

		lrPowerNeeded/= LowFrequencyThread.mrPreviousUpdateTimeStep;
		float lrPowerDiff = lrPowerNeeded - mrPPS;
		mrPPS += lrPowerDiff / 10f;//smooth it

	}
	// ****************************************************************************************************
	//This now loops mnHopperLoops times; the FetchEntity is likely to be the expensive part here
	int mnHopperLoops = 10;
	void CheckHoppers (long checkX, long checkY, long checkZ, Segment checkSegment, ushort lCube)
	{
		if (mPermissions == eHopperPermissions.AddOnly) return;//we are ADD, don't give our shit away
		if (mPermissions == eHopperPermissions.Locked) return;//ofc
		if (mnStorageUsed <=2) return;//no point in attempting to start


		// hoppers are a special case
		//TODO - obey our and linked hopper's permissions! --- probably not. <-why probably not?
		if (CubeHelper.HasEntity(lCube)) 
		{
			StorageMachineInterface lHopper = checkSegment.SearchEntity(checkX, checkY, checkZ) as StorageMachineInterface;

			if (lHopper != null) 
			{
				for (int i=0;i<mnHopperLoops;i++)
				{
					if (mnStorageUsed > 2)//we have at least 2 items
					{
						eHopperPermissions permissions = lHopper.GetPermissions();

						//only push things into the adjacent hopper if it's allowing it
						if (permissions == eHopperPermissions.Locked)
							return;
						if (permissions == eHopperPermissions.RemoveOnly)
							return;
						if (lHopper.RemainingCapacity > 1)//they have 1 spot
						{
							if (lHopper.UsedCapacity < mnStorageUsed - 1)//We have at least 2 more than them
							{
								//TODO: rewrite this to move items as well as cubes!
								ushort lType, lValue;
								GetSpecificCube (eHopperRequestType.eAny, out lType, out lValue);//get and remove <-why is this not the round robin version?

								if (lType != eCubeTypes.NULL)
								{
									
									if (!lHopper.TryInsert(this, lType, lValue, 1))
									{
										// TODO: Cope with failure
									}
									//give them a cube.
								}

								mrReadoutTick = 0.0f;
								//try again rapidly until we don't need to transfer
								//mnReadouts--;
								//ensure we check the SAME direction again
							}
						}
						else
						{
							return;
						}
					}
					else
					{
						return;
					}
				}
			}
		}
	}

	void CheckSuppliers(Segment checkSegment, ushort lCube, long checkX, long checkY, long checkZ)
	{
		if (mPermissions == eHopperPermissions.Locked || mPermissions == eHopperPermissions.RemoveOnly)
			return;

		// No point calling any suppliers if we have no space left anyway.
		if (mnStorageFree <= 0)
			return;

		var targetEntity = checkSegment.SearchEntity(checkX, checkY, checkZ) as StorageSupplierInterface;

		if (targetEntity != null)
		{
			targetEntity.ProcessStorageSupplier(this);
		}

//		if (lCube == eCubeTypes.OreExtractor)
//		{
//			//	Debug.Log("SH looking at OE with " + mnStorageFree + " free storage");
//			if (mnStorageFree > 0)
//			{
//				OreExtractor lExtractor = checkSegment.FetchEntity(eSegmentEntity.OreExtractor,checkX,checkY,checkZ) as OreExtractor;
//				if (lExtractor != null)
//					if (lExtractor.mnStoredOre >0)
//				{
//					int lnCount = lExtractor.mnStoredOre / 4;
//					if (lnCount <1) lnCount = 1;
//					if (lnCount > mnStorageFree) lnCount = mnStorageFree;//dont' attempt to remove more than we can fit!
//					//remove 1 ore into the storage hopper
//					//lExtractor.mnStoredOre-=lnCount;
//					for (int i=0;i<lnCount;i++)
//					{
//						AddCube(lExtractor.mnOreType, TerrainData.GetDefaultValue(lExtractor.mnOreType));
//						lExtractor.mnStoredOre--;
//						if (mnStorageFree == 0) break;
//					}
//					mrReadoutTick = 0.0f;//try again rapidly until we don't need to transfer
//					mnReadouts--;//ensure we check the SAME direction again
//				}
//			}
//		}				
//		
//		if (lCube == eCubeTypes.RefineryController)
//		{
//			//	Debug.Log("SH looking at OE with " + mnStorageFree + " free storage");
//			if (mnStorageFree > 0)
//			{
//				RefineryController controller = checkSegment.FetchEntity(eSegmentEntity.RefineryController,checkX,checkY,checkZ) as RefineryController;
//				if (controller != null)
//				{
//					if (controller.mOutputHopper != null)
//					{
//						int count = ItemManager.GetCurrentStackSize(controller.mOutputHopper);
//						
//						ItemBase item;
//						if (count > mnStorageFree)
//						{
//							item = controller.GetPartialInventory(mnStorageFree);
//						}
//						else
//						{
//							item = controller.GetWholeInventory();
//						}
//						
//						AddItem(item);
//						
//						// don't reset readout stuff, we're done already
//					}
//				}
//			}
//		}
		//really? Manufacuturing plants don't push, we pull?!
//		if (lCube == eCubeTypes.ManufacturingPlant)
//		{
//			if (mnStorageFree > 0)
//			{
//				ManufacturingPlant plant = checkSegment.FetchEntity(eSegmentEntity.ManufacturingPlant, checkX, checkY, checkZ) as ManufacturingPlant;
//
//				if (plant != null)
//				{
//					GetManufacturingPlantOutput(plant);
//				}
//			}
//		}
//		
//		if (lCube == eCubeTypes.ManufacturingPlantModule)
//		{
//			if (mnStorageFree > 0)
//			{
//				ManufacturingPlantModule module = checkSegment.FetchEntity(eSegmentEntity.ManufacturingPlantModule, checkX, checkY, checkZ) as ManufacturingPlantModule;
//				
//				if (module != null && module.mPlant != null)
//				{
//					GetManufacturingPlantOutput(module.mPlant);
//				}
//			}
//		}

//        if  (lCube == eCubeTypes.T4_Grinder)
//        {
//            T4_Grinder lGrinder = checkSegment.FetchEntity(eSegmentEntity.T4_Grinder,checkX,checkY,checkZ) as T4_Grinder;
//            if (lGrinder != null)//unlikely!
//            {
//                if (lGrinder.mLinkedCenter != null) lGrinder = lGrinder.mLinkedCenter;
//
//                lGrinder.CheckHopper(this);
//            }
//        }

//        if  (lCube == eCubeTypes.T4_GasBottler)
//        {
//            T4_GasBottler lGB = checkSegment.FetchEntity(eSegmentEntity.T4_GasBottler,checkX,checkY,checkZ) as T4_GasBottler;
//           if (lGB != null)//unlikely!
//            {
//                if (lGB.mLinkedCenter != null) lGB = lGB.mLinkedCenter;
//
//                lGB.CheckHopper(this);
//            }
//        }


        /*
        //Althought I don't like this, it avoids just having a massive local list of things for the MB machine to check.
        if  (lCube == eCubeTypes.T4_ParticleFilter)
        {
            T4_ParticleFilter lFilter = checkSegment.FetchEntity(eSegmentEntity.T4_ParticleFilter,checkX,checkY,checkZ) as T4_ParticleFilter;
            if (lFilter != null)//unlikely!
            {
                if (lFilter.mLinkedCenter != null) lFilter = lFilter.mLinkedCenter;

                lFilter.CheckHopper(this);
            }
        }*/
	}

//	void GetManufacturingPlantOutput(ManufacturingPlant plant)
//	{
//		if (plant.mOutputHopper != null)
//		{
//			int count = ItemManager.GetCurrentStackSize(plant.mOutputHopper);
//			
//			ItemBase item;
//			if (count > mnStorageFree)
//			{
//				item = plant.GetPartialInventory(mnStorageFree);
//			}
//			else
//			{
//				item = plant.GetWholeInventory();
//			}
//			RequestImmediateNetworkUpdate();
//			plant.RequestImmediateNetworkUpdate();
//			AddItem(item);
//		}
//	}

	//TODO: move this to the entities in question?
	void CheckConsumers(Segment checkSegment, ushort lCube, long checkX, long checkY, long checkZ)
	{
		if (mPermissions == eHopperPermissions.Locked || mPermissions == eHopperPermissions.AddOnly)
			return;

		var targetEntity = checkSegment.SearchEntity(checkX, checkY, checkZ) as StorageConsumerInterface;

		if (targetEntity != null)
		{
			targetEntity.ProcessStorageConsumer(this);
		}


//		if (lCube == eCubeTypes.CentralPowerHub)
//		{
//            if (WorldScript.mbIsServer)//DO NOT DO THIS FOR CLIENTS! 
//            {
//    			CentralPowerHub lHub = checkSegment.FetchEntity(eSegmentEntity.CentralPowerHub,checkX,checkY,checkZ) as CentralPowerHub;
//    			if (lHub != null)//unlikely!
//    			{
//    				//if (lHub.mrTimeLeftToConsumeMaterial <= 0.0f && lHub.mrNormalisedPower < 0.75f)
//    				if (lHub.WantsToConsumeResources())
//    				{
//    					//yay
//    					
//    					for (int i=0;i<mnMaxStorage;i++)
//    					{
//    						if (maStorage[i] != eCubeTypes.NULL)
//    						{
//    							if (CubeHelper.IsSmeltableOre(maStorage[i])) continue;//do not burn ore!
//
//    							if (!WorldScript.mLocalPlayer.mResearch.IsKnown(maStorage[i], 0)) //do not burn unknown materials
//    								continue;
//
//    							lHub.AddResourceToConsume(maStorage[i]);
//    							maStorage[i] = eCubeTypes.NULL;
//    							CountFreeSlots();
//    							MarkDirtyDelayed();
//    							return;
//    						}
//    					}
//
//    					for (int i=0;i<mnMaxStorage;i++)
//    					{
//    						if (maItemInventory [i] == null) continue;
//    						if (maItemInventory[i].mType != ItemType.ItemCubeStack) continue;
//    						
//    						if ((maItemInventory[i] as ItemCubeStack).mnAmount <=0) continue;//should have been nulled
//    						
//    						ushort lType = (maItemInventory[i] as ItemCubeStack).mCubeType;
//    						if (CubeHelper.IsSmeltableOre(lType)) continue;
//    						if (CubeHelper.IsHighCalorie(lType) == false) continue;
//    						
//    						lHub.AddResourceToConsume(lType);
//    						(maItemInventory[i] as ItemCubeStack).mnAmount--;
//    						if ((maItemInventory[i] as ItemCubeStack).mnAmount <=0) maItemInventory[i] = null;
//    						CountFreeSlots();
//    						MarkDirtyDelayed();
//    						return;
//    					}
//    				}
//                }
//			}
//		}
//		
//		if (lCube == eCubeTypes.PyrothermicGenerator)
//		{
//            if (WorldScript.mbIsServer)//DO NOT DO THIS FOR CLIENTS! 
//            {
//    			PyrothermicGenerator lPTG = checkSegment.FetchEntity(eSegmentEntity.PyrothermicGenerator,checkX,checkY,checkZ) as PyrothermicGenerator;
//    			if (lPTG != null)
//    			{
//    				if (lPTG.mbReadyForResource)
//    				{
//    					for (int i=0;i<mnMaxStorage;i++)
//    					{
//    						if (maStorage[i] == eCubeTypes.NULL) continue;
//    						if (CubeHelper.IsSmeltableOre(maStorage[i])) continue;
//    						if (CubeHelper.IsHighCalorie(maStorage[i]) == false) continue;
//    						//todo, do not burn things that aren't consumables for priority
//    						//todo, do not burn ore, ever! (except coal)
//    						if (maStorage[i] != eCubeTypes.NULL)
//    						{
//    							lPTG.AddResourceToConsume(maStorage[i]);
//    							maStorage[i] = eCubeTypes.NULL;
//    							CountFreeSlots();
//    							MarkDirtyDelayed();
//    							return;
//    						}
//    					}
//    					for (int i=0;i<mnMaxStorage;i++)
//    					{
//    						if (maItemInventory [i] == null) continue;
//    						if (maItemInventory[i].mType != ItemType.ItemCubeStack) continue;
//
//    						if ((maItemInventory[i] as ItemCubeStack).mnAmount <=0) continue;//should have been nulled
//
//    						ushort lType = (maItemInventory[i] as ItemCubeStack).mCubeType;
//    						if (CubeHelper.IsSmeltableOre(lType)) continue;
//    						if (CubeHelper.IsHighCalorie(lType) == false) continue;
//
//    						lPTG.AddResourceToConsume(lType);
//    						(maItemInventory[i] as ItemCubeStack).mnAmount--;
//    						if ((maItemInventory[i] as ItemCubeStack).mnAmount <=0) maItemInventory[i] = null;
//    						CountFreeSlots();
//    						MarkDirtyDelayed();
//    						return;
//    					}
//    				}
//    			}
//            }
//        }

        //This is *LOT* faster than the base round-robin-ing all of it's consumers
        //I like this paradigm, it saves wasiting round-robin time in a larger multiblock, but it means that this class is growing - does c# support Partial classes across multiple files?
        //Yes : http://answers.unity3d.com/questions/8009/does-unity-c-support-partial-classes.html
//        if  (lCube == eCubeTypes.SpiderBotBase)
//        {
//            SpiderBotBase lBase = checkSegment.FetchEntity(eSegmentEntity.SpiderBotBase,checkX,checkY,checkZ) as SpiderBotBase;
//            if (lBase != null)//unlikely!
//            {
//                if (lBase.mLinkedCenter != null) lBase = lBase.mLinkedCenter;
//
//                lBase.CheckHopper(this);
//            }
//        }

        if  (lCube == eCubeTypes.GeothermalGenerator)
        {
            GeothermalGenerator lGT = checkSegment.FetchEntity(eSegmentEntity.GeothermalGenerator,checkX,checkY,checkZ) as GeothermalGenerator;
            if (lGT != null)//unlikely!
            {
                if (lGT.mLinkedCenter != null) lGT = lGT.mLinkedCenter;

                lGT.CheckHopper(this);
            }
        }
  
      
	}

	Segment[,,] HooverSegment;

	void UpdateHoover()
	{
		if (WorldScript.mbIsServer == false) return;//this is done by the server
		/*if(mbHooverOn)
		{
			ToggleHoover();
			return;
		}*/

		if (mbHooverOn == false) return;

		if (HooverSegment == null)
		{
			HooverSegment = new Segment[3,3,3];//else don't even bother allocating it, 99.9% of hoppers are never used
		}

		//if we have storage and are not locked, suck in items
		if (mnStorageFree > 0 && mbHooverOn)
		{
			SegmentUpdater.mnNumHoovers++;
			//Attempt to hoover up items. This is more complex as they tend to be stacked

			for (int x=-1;x<=1;x++)
			{
				for (int y=-1;y<=1;y++)
				{
					for (int z=-1;z<=1;z++)
					{

						if (HooverSegment[x+1,y+1,z+1] == null)
						{

							long lookupX = mnX + (x * WorldHelper.SegmentX);
							long lookupY = mnY + (y * WorldHelper.SegmentY);
							long lookupZ = mnZ + (z * WorldHelper.SegmentZ);

							// As this was a bit expensive, an alternative version where we supply the frustrum is useful
							Segment segment = AttemptGetSegment(lookupX, lookupY, lookupZ);

							if (segment == null || !segment.mbInitialGenerationComplete || segment.mbDestroyed) return;//come back next frame, when this should be available

							HooverSegment[x+1,y+1,z+1] = segment;
							return;
						}

						
						//DroppedItemData droppedItem = ItemManager.instance.UpdateCollection(mnX, mnY + 1, mnZ, new Vector3(0.5f, 0, 0.5f), 4, 1.0f, 2.0f,mnStorageFree);
                        //This call should also remove the item from the segment
						DroppedItemData droppedItem = ItemManager.instance.UpdateCollectionSpecificSegment(mnX, mnY + 1, mnZ, new Vector3(0.5f, 0, 0.5f), 12, 1.0f, 2.0f, HooverSegment[x+1,y+1,z+1],mnStorageFree);
						if (droppedItem != null)
						{
							//we may need to split the object apart now?
							if (!AddItem(droppedItem.mItem))
							{
								//Object did not fit...uh - bad code caused by collection removing items even if it fails to fit!
								ItemManager.instance.DropItem (droppedItem.mItem,mnX,mnY+1,mnZ,Vector3.up);
								return;
							}
						}
					}
				}
			}
		}
		
	}
	
	// ***************************************************************************************************************************************
	int mnUpdates;
	float mrTimeUntilFlash;
	float mrTimeElapsed;
 
	public override void UnitySuspended ()
	{
		WorkLight = null;
		mHooverPart = null;
		mTextMesh = null;
		mHopperPart = null;
	}
	
	// ***************************************************************************************************************************************
	//This should do nothing if the segment isn't visible, else we're updating text and stuff when behind the player
	//~1.5ms for 400 hoppers
	//15ms for 1800 hopper

	int mnHooverEmissionRate;
	bool mbShowHopper;
    float mrPrevDistanceToPlayer;
	void UpdateLOD ()
	{
        
		bool lbShowHopper = true;
		if (mDistanceToPlayer > 64) lbShowHopper = false;
		if (mbWellBehindPlayer) lbShowHopper = false;
		if (Mathf.Abs(mVectorToPlayer.y) > 32.0f) lbShowHopper = false;
		if (mSegment.mbOutOfView) lbShowHopper = false;


		//Smartly do this work only when the vis has changed
		if (lbShowHopper != mbShowHopper)
		{
			mbShowHopper = lbShowHopper;
            mbForceHoloUpdate = true;//Ensure the vis is correct for this
            if (lbShowHopper == true)
		{
				if (mHopperPart.activeSelf == false)
			{
					mHopperPart.SetActive(true);
					
					mHopperPart.GetComponent<Renderer>().enabled = true;
			}
			}
			else
			{
				if (mHopperPart.activeSelf == true)
				{
					mHopperPart.SetActive(false);
			}
				mHopperPart.GetComponent<Renderer>().enabled = false;//hopefully this overrides the LOD stuff - I wish everything used LODs and culled at the same point :/
				//But if we did that, it's every frame on the main thread and fuck unity in it's stupid face
		}
		
		}


		UpdateHooverEmission();
		
		//disable text at unreadable distances
		if (mDistanceToPlayer > 24.0f || mbWellBehindPlayer || mDistanceToPlayer > CamDetail.SegmentDrawDistance-8)
		{
			if (mTextMesh.GetComponent<Renderer>().enabled == true)
			{
				mbForceHoloUpdate = true;
				mTextMesh.GetComponent<Renderer>().enabled = false;
	    	}
		}
		else
		{
			if (mTextMesh.GetComponent<Renderer>().enabled == false)
			{
				mbForceHoloUpdate = true;
				mTextMesh.GetComponent<Renderer>().enabled = true;
		    }
		}
		
		

	}

	void UpdateHooverEmission()
		{
		
		bool lbAllowHoover = true;
		
		if (mDistanceToPlayer > 16.0f || mbWellBehindPlayer) lbAllowHoover = false;
		if (mDistanceToPlayer > (CamDetail.SegmentDrawDistance) || Mathf.Abs(mVectorToPlayer.y) > 24.0f || mbWellBehindPlayer) lbAllowHoover = false;
		
		if (lbAllowHoover == false)
		{
			if (mnHooverEmissionRate > 0)
			{
				mnHooverEmissionRate--;
				mHooverPart.emissionRate = mnHooverEmissionRate;
		}
			
			//mHooverPart.gameObject.SetActive(false);
		}
		else
		{
			if (mbHooverOn)
			{
				
				if (mnHooverEmissionRate <= 10)
				{
					mnHooverEmissionRate++;
					mHooverPart.emissionRate = mnHooverEmissionRate;
		}
			}
			else
			{
				if (mnHooverEmissionRate > 0)
				{
					mnHooverEmissionRate--;
					mHooverPart.emissionRate = mnHooverEmissionRate;
				}
		
	}
			//mHooverPart.gameObject.SetActive(true);
		}
	}

	float mrMaxLightDistance = 32;

	//Ho hum, too many lights; 12ms+ on i7 to cull...
	void UpdateWorkLight ()
	{

		bool lbLightShouldBeEnabled = false;
		if (mnStorageUsed == 0) lbLightShouldBeEnabled = true;
		if (mnStorageFree == 0) lbLightShouldBeEnabled = true;
		if (mValue == CRYO_HOPPER) lbLightShouldBeEnabled = true;

		if (mbWellBehindPlayer) lbLightShouldBeEnabled = false;

		mrMaxLightDistance += (CamDetail.FPS - mrMaxLightDistance) * Time.deltaTime * 0.1f;//very slow lerp based on framerate
		if (mrMaxLightDistance < 2) mrMaxLightDistance = 2;
		if (mrMaxLightDistance > 64) mrMaxLightDistance = 64;

		if (mDistanceToPlayer > mrMaxLightDistance) lbLightShouldBeEnabled = false;//I wonder if this could be a very smoothed number that follows the framerate...?


		if (lbLightShouldBeEnabled)
		{
			if (WorkLight.enabled == false)
			{
				WorkLight.enabled = true;
				WorkLight.range = 0.05f;//fade up, don't just appear.(this is the minimum not to get immediately turned off)
			}

			if (mValue == CRYO_HOPPER) //blue/red to quickly show temperature. Could also lerp, but nah.
			{
				if (mrCurrentTemperature > SAFE_COLD_TEMP)
				{
					WorkLight.color = Color.Lerp(WorkLight.color,Color.red,Time.deltaTime);
					WorkLight.range += 0.1f;
				}
				else
				{
					WorkLight.color = Color.Lerp(WorkLight.color,Color.cyan,Time.deltaTime);
					WorkLight.range += 0.1f;
				}
			}
			else
			{
				//Not a Cryo hopper, simply show full/empty as green/red
				if (mnStorageUsed == 0)
				{
					WorkLight.color = Color.Lerp(WorkLight.color,Color.green,Time.deltaTime);//1 second to smoothly lerp? Maybe?
					WorkLight.range += 0.1f;
				}
				else
				{
					if (mnStorageFree == 0)
					{
						WorkLight.color = Color.Lerp(WorkLight.color,Color.red,Time.deltaTime);//1 second to smoothly lerp? Maybe?	
						WorkLight.range += 0.1f;

					}
					else
					{
						//this should probably actually be lbLightShouldBeEnabled = false
						WorkLight.color = Color.Lerp(WorkLight.color,Color.cyan,Time.deltaTime);//1 second to smoothly lerp? Maybe?
						WorkLight.range -= 0.1f;//That's ok, you're working nicely, don't need to indicate
					}
				}
			}

			if (WorkLight.range > 1.0f) WorkLight.range  = 1.0f;//this is automatically reduced to 95% a bit below here, so it's technically oscillating a bit
		}
		else
		{
			//Do not disable light directly; light will fade rapidly and disable itself (Light does not need to be disabled, for whatever reason)
		}

		if (WorkLight.enabled)
		{
			if (WorkLight.range < 0.15f) 
	{
				WorkLight.enabled = false;//maybe as much as .2? (turning this off sooner gives performance back sooner)
			}
		else
		{
				WorkLight.range*=0.95f;
			}
		}
	}

	//Does its own Vis

	eHopperPermissions mPreviousPermissions = eHopperPermissions.eNumPermissions;

    MaterialPropertyBlock mHoloMPB;


	//Almost an MS of frametime in here - have a serious think about a better way of doing this 
    //I HAD A SERIOUS THINK NOT REALLY BUT I CAME UP WITH A BETTER IDEA
	void SetHoloStatus()
	{
		if (mHoloStatus == null) return;//I fail

		if (mbForceHoloUpdate) mPreviousPermissions = eHopperPermissions.eNumPermissions;
		mbForceHoloUpdate = false;

        if (mDistanceToPlayer > 8 || mbWellBehindPlayer) 
        {
            if (mHoloStatus.activeSelf) mHoloStatus.SetActive(false);
            return;
        }
        if (mHoloStatus.activeSelf == false) 
        {
			mPreviousPermissions = eHopperPermissions.eNumPermissions;
            mHoloStatus.SetActive(true);
        }

        if (mPreviousPermissions != mPermissions)
        {
            mPreviousPermissions = mPermissions;//only do this if the permissions have changed
            //cache this if necessary
			if (mPermissions == eHopperPermissions.AddAndRemove)  mHoloStatus.GetComponent<Renderer>().material.mainTextureOffset = new Vector2(0.0f,0.5f);
			if (mPermissions == eHopperPermissions.RemoveOnly)    mHoloStatus.GetComponent<Renderer>().material.mainTextureOffset = new Vector2(0.0f,0.0f);
			if (mPermissions == eHopperPermissions.Locked)        mHoloStatus.GetComponent<Renderer>().material.mainTextureOffset = new Vector2(0.5f,0.0f);
			if (mPermissions == eHopperPermissions.AddOnly)       mHoloStatus.GetComponent<Renderer>().material.mainTextureOffset = new Vector2(0.5f,0.5f);
         }
	}

	//only update if the text is being disabled
	void UpdateMeshText()
	{
		if (mTextMesh.GetComponent<Renderer>().enabled && mDistanceToPlayer < 12)//we can see text up to 24m right now, but there's little point, it's not readable
		{
			string lText = mPermissions.ToString() + "\n";
			
			if (mnStorageFree == 0) lText += "Storage full\n";
			else
				if (mnStorageUsed == 0) lText += "Storage Empty\n";
			else
				lText  += mnStorageFree.ToString() + " free slots\n";//
			
			if (mrTimeSinceLogistics > 0.0f)
				lText += "Processing...";
			else
				lText += "[" + mLastItemAdded +"]";
			
			mTextMesh.text = lText;

			mbForceTextUpdate = false;
		}
	}
	void LinkToGO()
	{
			if (mWrapper == null || !mWrapper.mbHasGameObject) 
			{
				return;
			}
			else
			{
				if (mWrapper.mGameObjectList == null) Debug.LogError("Ore Extractor missing game object #0?");
				if (mWrapper.mGameObjectList[0].gameObject == null) Debug.LogError("Ore Extractor missing game object #0 (GO)?");
				//WorkLight = mWrapper.mGameObjectList[0].gameObject.GetComponentInChildren<Light>();
				WorkLight = mWrapper.mGameObjectList[0].transform.Search("HooverGraphic").GetComponent<Light>();
				if (WorkLight == null) Debug.LogError("Storage Hopper has missing light?");
				//if (QualitySettings.GetQualityLevel() >=3) WorkLight.shadows = LightShadows.Hard;//Object casts shadows and we're inside the object. Sad times.

				mHooverPart = mWrapper.mGameObjectList[0].transform.Search("HooverGraphic").GetComponent<ParticleSystem>();
			    mHooverPart.emissionRate = 0;
				//mHooverPart= mWrapper.mGameObjectList[0].gameObject.GetComponentInChildren<ParticleSystem>();
			
			    mTextMesh = mWrapper.mGameObjectList[0].gameObject.transform.Search("Storage Text").GetComponent<TextMesh>();//this is like HUNDREDS of times faster than the below call..
			//.GetComponentInChildren<TextMesh>();s
				mHopperPart = mWrapper.mGameObjectList[0].transform.Find("Hopper").gameObject;



				mHoloStatus 	= mHopperPart.transform.Find("Holo_Status").gameObject;

				mHoloStatus.SetActive(false);
                mPreviousPermissions = eHopperPermissions.eNumPermissions;//force an update
				SetHoloStatus();

				mbForceTextUpdate = true;

				mbLinkedToGO = true;

                //mHoloMPB = new MaterialPropertyBlock();
			}
		}
	bool mbTutorialComplete = false;
	void ConfigTutorial()
	{
		if (mbTutorialComplete) return;//the two equality checks below were upto 0.73ms/frame on a big world.
		//add/remove tutorial
		if (WorldScript.meGameMode == eGameMode.eSurvival)
		{
			if (SurvivalPlayerScript.meTutorialState == SurvivalPlayerScript.eTutorialState.RemoveCoalFromHopper)
			{
				if (TutorialEffect == null)
				{
					TutorialEffect = (GameObject)GameObject.Instantiate(SurvivalSpawns.instance.EmptySH,
					                                                    mWrapper.mGameObjectList[0].gameObject.transform.position + Vector3.up + Vector3.up,
					                                                    Quaternion.identity);
					
					TutorialEffect.SetActive(true);
				}
			}
			else
			{
				if (TutorialEffect != null)
				{
					GameObject.Destroy(TutorialEffect);
					TutorialEffect = null;
					mbTutorialComplete = true;
				}
			}
		}
	}
	public override void UnityUpdate()
	{
		mrTimeElapsed += Time.deltaTime;
		if (!mbLinkedToGO)
		{
			LinkToGO();
			return;
		}

		ConfigTutorial();

		if (mbWellBehindPlayer)
		{
		}
		else
		{
			if (mSegment.mbOutOfView)
			{
			}
			else
			{
                //When we straddle the 8m distance at which we render the holothing, then force an update
                if (mDistanceToPlayer <=8 && mrPrevDistanceToPlayer >8) mbForceHoloUpdate = true;
                if (mDistanceToPlayer >8 && mrPrevDistanceToPlayer <=8) mbForceHoloUpdate = true;
				//if (mnUpdates % 60 == 0) 
				{
					if (mbForceHoloUpdate)// || mnUpdates % 30 == 0)//just because there's no good boundary/threshold we cross to check this (other than storing player distance last and this frame and comparing)
					{
						SetHoloStatus();
					}
					if (mbForceTextUpdate)
					{
						UpdateMeshText();
					}
				}
			}
		}
		
		
			

		mnUpdates++;
		UpdateLOD();
		
		//bah
	/*	if (mPermissions == ePermissions.Locked) mTextMesh.color  = Color.red;
		if (mPermissions == ePermissions.AddOnly) mTextMesh.color  = Color.blue;
		if (mPermissions == ePermissions.RemoveOnly) mTextMesh.color  = Color.cyan;
		if (mPermissions == ePermissions.AddAndRemove) mTextMesh.color  = Color.green;
	*/	
		
		UpdateWorkLight ();
		
	
			}
	bool mbHooverEmissionOff;//mHooverPart.emissionRate is using up 0.06ms on mainthread... fuck's sake guys
	// ***************************************************************************************************************************************
	public void AddCube(ushort lType, ushort lValue)
	{
		if (lType == eCubeTypes.NULL) Debug.LogError("Who and why is someone adding NULL to a Storage Hopper?" + System.Environment.StackTrace);
		if (mnStorageFree <= 0) 
		{
			Debug.LogError("Error, can't AddCube "+ lType +" to hopper, it's full!");
			return;

		}
		
		int firstEmpty = mnMaxStorage;
		for (int i=0;i<mnMaxStorage;i++)
		{
			ItemBase item = maItemInventory[i];
			if (item == null)//we have a free slot to add into!
			{
				if (i < firstEmpty)
					firstEmpty = i;

                if (WorldScript.mLocalPlayer.mResearch.IsKnown(lType, 0))
				{
                    mLastItemAdded = TerrainData.GetNameForValue(lType,lValue);
                        //TerrainData.mEntries[lType].Name;
				}
				else
				{
					mLastItemAdded = PlayerResearch.UnknownMaterial;
				}

				break;
			}
			if (item.mType == ItemType.ItemCubeStack)
			{
				ItemCubeStack stack = item as ItemCubeStack;


				if (stack == null)
				{
					Debug.LogError("Error, failed to convert item into ItemCubeStack" + item.mType + ":" + item.mnItemID);
					return;
				}

				if (stack.mCubeType == lType && stack.mCubeValue == lValue)
				{
					stack.mnAmount ++;

					if (WorldScript.mLocalPlayer.mResearch.IsKnown(lType, 0))
					{
						if (lType >= TerrainData.mEntries.Length)
						{
							Debug.LogError("Error, AddCube tried to get terrain data entry for" + lType + " but max was only " + TerrainData.mEntries.Length);
							mLastItemAdded = "ERROR Unknown cube[" + lType + "] added";
						}
						else
						{
                            mLastItemAdded = TerrainData.GetNameForValue(lType,lValue);
                            /*
							TerrainDataEntry lEntry = TerrainData.mEntries[lType];
							if (lEntry == null)
							{
								mLastItemAdded = "ERROR Unknown cube[" + lType + "] added";
							}
							else
							{
								mLastItemAdded = lEntry.Name;
							}*/
						}
					}
					else
					{
						mLastItemAdded = PlayerResearch.UnknownMaterial;
					}

					MarkDirtyDelayed();
					CountFreeSlots();
					return;
				}
			}
		}
		
		if (firstEmpty == mnMaxStorage)
		{
			// we had storage free, but no item was added??
			Debug.Log("Attempted to add to Storage Hopper and failed miserably!");
			return;
		}
		
		maItemInventory[firstEmpty] = ItemManager.SpawnCubeStack(lType, lValue, 1);
		CountFreeSlots();
		MarkDirtyDelayed();

	}
	//This will actually help out with disk space too
    //Warning - untested!
    public void CollapseContents()
    {
        for (int x = 0;x < mnMaxStorage; x++)
        {
            for (int y = 0;y < mnMaxStorage; y++)
            {
                if (x == y) continue;//heh

                if (ItemManager.StackWholeItems(maItemInventory[x], maItemInventory[y], false))
                {
                    Debug.LogError("Added " + maItemInventory[y].ToString() + " to " + maItemInventory[x]);
                    maItemInventory[y] = null;
                }
            }
        }
    }

	public void IterateContents(IterateItem itemFunc, object state)
	{
		if (itemFunc == null) return;

	
		for (int i = 0;i < mnMaxStorage; i++)
		{
			ItemBase item = maItemInventory[i];
			if (item == null) continue;
			if (!itemFunc(maItemInventory[i], state))
				return;
		}
		
	}

	public int CountHowManyOfOreType(ushort lType)
	{
		if (CubeHelper.IsOre(lType) == false) Debug.LogError("This is only for Ores and other things that we DO NOT CARE ABOUT THE VALUE OF");
		
		//Rewrite to use item inventory and actually check value!
		int lnNumFound = 0;
		
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;
			if (maItemInventory[i].mType == ItemType.ItemCubeStack)
			{
				ItemCubeStack stack = (maItemInventory[i] as ItemCubeStack);
				
				if (stack.mCubeType == lType)
				{
					lnNumFound += stack.mnAmount;
				}
			}
		}
		
		return lnNumFound;
	}
	
	// ***************************************************************************************************************************************
	//Now abstracts Cubes and CubeStacks together
	//Pass in ushort.MaxValue as the Value to get any value of this type back
	public int CountHowManyOfType(ushort lType, ushort lValue)
	{
		
		//Rewrite to use item inventory and actually check value!
		int lnNumFound = 0;
		
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;
			if (maItemInventory[i].mType == ItemType.ItemCubeStack)
			{
				ItemCubeStack stack = (maItemInventory[i] as ItemCubeStack);

				if (lValue == ushort.MaxValue)
				{
					//any Value is valid!
					if (stack.mCubeType == lType)
					{
						lnNumFound += stack.mnAmount;
					}
				}

				if (stack.mCubeType == lType && stack.mCubeValue == lValue)
				{
					lnNumFound += stack.mnAmount;
				}
			}
		}
				
		return lnNumFound;
	}
	// ***************************************************************************************************************************************
	public int CountHowManyOfItem (int itemID)
	{
		int lnNumFound = 0;
		
		for (int i=0;i<mnMaxStorage;i++)
		{
			ItemBase item = maItemInventory[i];
			
			if (item == null)
				continue;
			
			if (item.mnItemID != itemID)
				continue;
			
			ItemStack stack = item as ItemStack;
			
			if (stack != null)
			{
				lnNumFound += stack.mnAmount;
			}
			else
			{
				lnNumFound++;
			}
		}
				
		return lnNumFound;		
	}
	
	// ***************************************************************************************************************************************
	//look for some ore; if we find some, count it up; allow querying of type
	//If Type is is Null, then return the Ore we have the most of
	//This only returns Ore that can be converted into Ingots.
	public int ContainsOre(ushort lSearchType, bool knownOnly, List<CraftData> recipes, out ushort lBestChoice)
	{

		if (lSearchType != eCubeTypes.NULL)
		{
			//Just return how many of this we have
			lBestChoice = lSearchType;
			return CountHowManyOfOreType(lSearchType);
		}

		int lnNumFound = 0;
		lBestChoice = eCubeTypes.NULL;

		//if lSearchType is null, then look for any ore

		//now search again, but take into account ItemCubeStacks
		//The issue is that CountHowManyOfType abstracts items and cubes.
		//So we need to only take account thigns that did not exist as cubes.
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;
			if (maItemInventory[i].mType != ItemType.ItemCubeStack) continue;
			if ((maItemInventory[i] as ItemCubeStack).mnAmount <=0) continue;

			ItemCubeStack lStack = (maItemInventory[i] as ItemCubeStack);

			if (lStack == null)
			{
				Debug.LogError("Error stack was null in Contains Ore?");
				continue;
			}
            if (WorldScript.mLocalPlayer == null)//what even?
            {
                continue;
            }
			if (WorldScript.mLocalPlayer.mResearch == null)
			{
				Debug.LogError("mResearch was null in Contains Ore?");
				continue;
			}

			//If we already deemed this the best type, then skip it; we've already counted it up
			if (lStack.mnItemID == lBestChoice) continue;

			if (CubeHelper.IsIngottableOre(lStack.mCubeType))
			{
				bool validRecipe = true;
				
				if (recipes != null)
				{
					validRecipe = false;
					
					// Also check we have a recipe for this ore.
					foreach(CraftData recipe in recipes)
					{
						foreach(CraftCost cost in recipe.Costs)
						{
							if (cost.CubeType == lStack.mCubeType)
							{
								validRecipe = true;
								break;
							}
						}
						
						if (validRecipe)
							break;
					}
				}
				
				if (validRecipe)
				{
					if (knownOnly && !WorldScript.mLocalPlayer.mResearch.IsKnown(lStack.mCubeType, 0))
					{
						continue; // Ignore unknown ore.
					}

					int lnNumStored = CountHowManyOfOreType(lStack.mCubeType);
						//CountHowManyOfType(lStack.mCubeType,TerrainData.GetDefaultValue(lStack.mCubeType));
					
					if (lnNumStored > lnNumFound)
					{
						lBestChoice = lStack.mCubeType;
						lnNumFound = lnNumStored;
					}
				}
			}
		}
		
		return lnNumFound;
	}
	
	// ***************************************************************************************************************************************
    public bool RemoveInventoryCube(ushort lType) // DEPRECATED DO NOT USE. DON'T USE ADAM. This is called by GetSpecificCube still, which MMs use - Dj
	{
		if (mnStorageUsed == 0) return false;
		
		//Now check for CubeStacks of this type, and decrement
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;

			if (maItemInventory[i].mType == ItemType.ItemCubeStack)
			{
				ItemCubeStack lInvStack = maItemInventory[i] as ItemCubeStack;
				if (lInvStack.mnAmount <=0) continue;
				if (lInvStack.mCubeType != lType) continue;

				lInvStack.mnAmount--;
				if (lInvStack.mnAmount<=0)
				{
					maItemInventory[i] = null;
				}

				CountFreeSlots();	
				MarkDirtyDelayed();

				return true;
			}
		}

		return false;
	}
	// ******************************************************************************************
	// returns how many were actually removed
	public int RemoveInventoryCube(ushort lType, ushort lValue, int amount)
	{
		if (mnStorageUsed == 0) return 0;

		int amountToRemove = amount;
		for (int i = 0; i < mnMaxStorage; i++)
		{
			ItemBase item = maItemInventory[i];

			if (item != null && item.mnItemID == -1)
			{
				ItemCubeStack stack = item as ItemCubeStack;

				if (stack.mCubeType == lType && stack.mCubeValue == lValue)
				{
					if (stack.mnAmount <= amountToRemove)
					{
						maItemInventory[i] = null;
						amountToRemove -= stack.mnAmount;

						if (amountToRemove == 0)
							break;
					}
					else
					{
						stack.mnAmount -= amountToRemove;
						amountToRemove = 0;
						break;
					}
				}
			}
		}
		
		if (amountToRemove < amount)
		{
            RequestImmediateNetworkUpdate();
			CountFreeSlots();	
			MarkDirtyDelayed();
			if (mnStorageUsed == 0) mLastItemAdded = "Empty";
		}
		
		return amount - amountToRemove;
	}
	// ***************************************************************************************************************************************

	// returns how many of item were actually stored
	public int StoreItem(ItemBase itemToUnload)
	{
        if (itemToUnload == null) Debug.LogError("Error, (Cargo Lift?)attempted to store a null item!");
		if (mnStorageFree == 0)
			return 0;

		int count = ItemManager.GetCurrentStackSize(itemToUnload);

		int itemsToStore = Mathf.Min(count, mnStorageFree);
		int itemsLeft = itemsToStore;
        //this will ONLY attempt to stack partial items, and will not put anything into empty slots.
		for (int i = 0; i < maItemInventory.Length; i++)
		{
			ItemBase existingItem = maItemInventory[i];
            if (existingItem == null) continue;

			if (existingItem.mType == itemToUnload.mType)
			{
				int stackAmount = ItemManager.StackPartialItems(existingItem, itemToUnload, itemsLeft);

				itemsLeft -= stackAmount;

				if (itemsLeft == 0)
				{
					break;
				}
			}
		}

        if (itemsLeft != 0)
        {
            CountFreeSlots();
            if (mnStorageFree > 0)
            {
                if (AddItem(itemToUnload))
                {
                    itemsLeft = 0;
                }
                else
                {
                    //This means that there was potentially a free slot, but the stack size left was too big?
                    //So we'll attempt to fill up a new slot

                    for (int i = 0; i < maItemInventory.Length; i++)
                    {
                        if (maItemInventory[i] == null)
                        {
                            int lnMax = ItemManager.GetMaxStackSize(itemToUnload);  //BIG NUMBER
                            if (lnMax > itemsLeft) lnMax = itemsLeft;               //The amount we have left
                            if (lnMax > mnStorageFree) lnMax = mnStorageFree;       //The amount we could actually store


                            Debug.LogWarning("StoreItem is storing [" + itemToUnload.ToString() + "] and has " + lnMax + " slots to mess around with!");

                            ItemBase lItem = ItemManager.CloneItem(itemToUnload);
                            ItemManager.SetItemCount(lItem,0);
                            maItemInventory[i] = lItem;//at this point, we have a new item of count 0

                            int stackAmount = ItemManager.StackPartialItems(lItem, itemToUnload, lnMax);

                            itemsLeft -= stackAmount;

                            if (itemsLeft == 0)
                            {
                                break;
                            }
                            CountFreeSlots();
                        }
                    }

                    Debug.LogWarning("Stacked and stored "+ (itemsToStore - itemsLeft) + " items, but then failed to store the remaining " + itemToUnload.ToString() + " in the storage hopper!");
                }
            }
        }

        if ( itemsLeft < 0) Debug.LogError("Error, Store Item removed more than max!");

        FinaliseHopperChange();

		return itemsToStore - itemsLeft;
	}
	
		// ***************************************************************************************************************************************
	//todo, return Value too
	

	// ********************************************************
	//This ensures (or at least massively improves) the return of items
	int mnRoundRobinOffset;
	public void GetSpecificCubeRoundRobin(eHopperRequestType lType, out ushort cubeType, out ushort cubeValue)
	{
		if (lType == eHopperRequestType.eNone)
		{
			cubeType = eCubeTypes.NULL;
			cubeValue = 0;
			return;
		}
		if (mnStorageUsed == 0 || lType == eHopperRequestType.eBarsOnly || lType == eHopperRequestType.eAnyCraftedItem)
		{
			cubeType = eCubeTypes.NULL;
			cubeValue = 0;
			return;	//this is an item
		}
		for (int l=0;l<mnMaxStorage;l++)
		{
			mnRoundRobinOffset++;
			mnRoundRobinOffset %= mnMaxStorage;

			int i = mnRoundRobinOffset;

			if (maItemInventory[i] == null) continue;

			if (maItemInventory[i].mType != ItemType.ItemCubeStack) continue;
			ItemCubeStack lStack = maItemInventory[i] as ItemCubeStack;

			if (lType == eHopperRequestType.eOrganic) continue;//Unsure if we have any organic blocks yet?

			if (lType == eHopperRequestType.eHighCalorieOnly)
				if (!CubeHelper.IsHighCalorie (lStack.mCubeType)) continue;
			
			if (lType == eHopperRequestType.eOreOnly)
				if (!CubeHelper.IsSmeltableOre(lStack.mCubeType)) continue;
			
			if (lType == eHopperRequestType.eGarbage)
				if (!CubeHelper.IsGarbage(lStack.mCubeType)) continue;
			
			if (lType == eHopperRequestType.eCrystals)
				if (lStack.mCubeType != eCubeTypes.OreCrystal) continue;
			
			if (lType == eHopperRequestType.eGems)
				if (lStack.mCubeType != eCubeTypes.Crystal) continue;
			
			if (lType == eHopperRequestType.eBioMass)
				if (lStack.mCubeType != eCubeTypes.OreBioMass) continue;
			
			if (lType == eHopperRequestType.eSmeltable)
				if (!CubeHelper.IsIngottableOre(lStack.mCubeType)) continue;

			if (lType == eHopperRequestType.eResearchable)
			{
				TerrainDataEntry entry = TerrainData.mEntries[lStack.mCubeType];
				if (entry == null || entry.DecomposeValue <= 0) continue;
			}


			RemoveInventoryCube(lStack.mCubeType, lStack.mCubeValue, 1);
			if (mnStorageUsed == 0) mLastItemAdded = "Empty";
			cubeType = lStack.mCubeType;
			cubeValue = lStack.mCubeValue;
			return;
				
		
		}
		//Didn't find anything. For shame.
		cubeType = eCubeTypes.NULL;	
		cubeValue = 0;
	}
	// ********************************************************
	public void GetSpecificCube(eHopperRequestType lType, out ushort cubeType, out ushort cubeValue)
	{
		if (lType == eHopperRequestType.eNone)
		{
			cubeType = eCubeTypes.NULL;
			cubeValue = 0;
			return;
		}
		if (mnStorageUsed == 0 || lType == eHopperRequestType.eBarsOnly || lType == eHopperRequestType.eAnyCraftedItem)
		{
			cubeType = eCubeTypes.NULL;
			cubeValue = 0;
			return;	//this is an item
		}

		//Now remove any itemcubes
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;
			if (maItemInventory[i].mType != ItemType.ItemCubeStack) continue;
			ItemCubeStack lStack = maItemInventory[i] as ItemCubeStack;

			if (lType == eHopperRequestType.eHighCalorieOnly)
				if (!CubeHelper.IsHighCalorie (lStack.mCubeType)) continue;
			
			if (lType == eHopperRequestType.eOreOnly)
				if (!CubeHelper.IsSmeltableOre(lStack.mCubeType)) continue;
			
			if (lType == eHopperRequestType.eGarbage)
				if (!CubeHelper.IsGarbage(lStack.mCubeType)) continue;

			if (lType == eHopperRequestType.eCrystals)
				if (lStack.mCubeType != eCubeTypes.OreCrystal) continue;

			if (lType == eHopperRequestType.eGems)
				if (lStack.mCubeType != eCubeTypes.Crystal) continue;

			if (lType == eHopperRequestType.eBioMass)
				if (lStack.mCubeType != eCubeTypes.OreBioMass) continue;

			if (lType == eHopperRequestType.eSmeltable)
				if (!CubeHelper.IsIngottableOre(lStack.mCubeType)) continue;
			
			if (lType == eHopperRequestType.eResearchable)
			{
				TerrainDataEntry entry = TerrainData.mEntries[lStack.mCubeType];
				if (entry == null || entry.DecomposeValue <= 0) continue;
			}

			RemoveInventoryCube(lStack.mCubeType, lStack.mCubeValue, 1);
			if (mnStorageUsed == 0) mLastItemAdded = "Empty";
			cubeType = lStack.mCubeType;
			cubeValue = lStack.mCubeValue;
			return;

		}

		cubeType = eCubeTypes.NULL;	
		cubeValue = 0;
	}
	// ********************************************************
	public ItemBase RemoveSingleSpecificCubeStack(ItemCubeStack lItem, bool lbInvertSearch = false)
	{
		if (lItem == null)
		{
			Debug.LogError("There's probably a good reason why RemoveSingleSpecificCubeStack is looking for a null Item");
			return null;
		}

		if (mnStorageUsed == 0)
		{
			return null;//early out
		}

		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;

			if (maItemInventory[i].mType != ItemType.ItemCubeStack) continue;

			ItemCubeStack lStack = maItemInventory[i] as ItemCubeStack;

			bool lbFound = false;

			if (lbInvertSearch == false && lStack.mCubeType == lItem.mCubeType && (lStack.mCubeValue == lItem.mCubeValue || lItem.mCubeValue == ushort.MaxValue))
				lbFound = true;

			if (lbInvertSearch == true)
			{
				if (lStack.mCubeType != lItem.mCubeType) lbFound = true;//a different type, value isn't relevant
				if (lStack.mCubeValue != lItem.mCubeValue) lbFound = true;//a different value, type isn't relevant
			}
			 

			if (lbFound)
			{
				ItemBase lRet = DecrementInventorySlot(i);
				if (lRet != null) 
				{
					CountFreeSlots();
					MarkDirtyDelayed();
					return lRet;
				}
			}
		}
		return null;
	}
	// ********************************************************
	public ItemBase RemoveSingleSpecificItemByID(int lnItemID, bool lbInvertSearch = false)
	{
		if (lnItemID == -1)
		{
			Debug.LogError("There's probably a good reason why RemoveSingleSpecificItemByID is looking for ItemID -1");
			return null;
		}
		if (mnStorageUsed == 0)
		{
			return null;//early out
		}
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;

			if (lbInvertSearch)
			{
				if (maItemInventory[i].mnItemID != lnItemID)
				{
					ItemBase lRet = DecrementInventorySlot(i);
					if (lRet != null) 
					{
						CountFreeSlots();
						MarkDirtyDelayed();
						return lRet;
					}
				}
			}
			else
			{

				if (maItemInventory[i].mnItemID == lnItemID)
				{
					ItemBase lRet = DecrementInventorySlot(i);
					if (lRet != null) 
					{
						CountFreeSlots();
						MarkDirtyDelayed();
						return lRet;
					}
				}
			}
		}
		return null;
	}
	// ********************************************************
	public ItemBase RemoveSingleSpecificItemOrCubeRoundRobin(eHopperRequestType lType)
	{
		if (lType == eHopperRequestType.eNone) 			return null;
		if (lType == eHopperRequestType.eHighCalorieOnly) return null;
		if (lType == eHopperRequestType.eOreOnly)			return null;
		if (lType == eHopperRequestType.eGarbage)			return null;
		if (lType == eHopperRequestType.eHighCalorieOnly)	return null;
		//	if (lType == eRequestType.eAnyCraftedItem)	return null;//This... should be ok? (or has martijns cube change fucked it?)
		
		ItemBase lRet = null;
		
		//we do this even if the mnStorageUsed is 0?
		
		for (int l=0;l<mnMaxStorage;l++)
		{
			mnRoundRobinOffset++;
			mnRoundRobinOffset %= mnMaxStorage;
			
			int i = mnRoundRobinOffset;

			if (maItemInventory[i] == null) continue;

	
			
			//we have no handy way of sub-dividing items into classes or tags
			//For the moment, just search for the most obvious things
			if (lType == eHopperRequestType.eAny)
			{
				lRet = DecrementInventorySlot(i);
				if (lRet != null) 
				{
					CountFreeSlots();
					MarkDirtyDelayed();
					return lRet;
				}
			}
			else
			{
				if (maItemInventory[i].mType == ItemType.ItemStack)
				{
					int lnID = maItemInventory[i].mnItemID;
					if (lType == eHopperRequestType.eOrganic)
					{
						//Ruined/Pristine parts
						if (lnID >= 4000 && lnID <= 4101)
						{
							lRet = DecrementInventorySlot(i);
							if (lRet != null) 
							{
								CountFreeSlots();
								MarkDirtyDelayed();
								return lRet;
							}	
						}
					}
					if (lType == eHopperRequestType.eBarsOnly)
					{
						bool lbIsBar = false;
						if (lnID == ItemEntries.CopperBar) lbIsBar = true;
						if (lnID == ItemEntries.TinBar) lbIsBar = true;
						if (lnID == ItemEntries.IronBar) lbIsBar = true;
						if (lnID == ItemEntries.LithiumBar) lbIsBar = true;
						if (lnID == ItemEntries.GoldBar) lbIsBar = true;
						if (lnID == ItemEntries.NickelBar) lbIsBar = true;
						if (lnID == ItemEntries.TitaniumBar) lbIsBar = true;
                        if (lnID == ItemEntries.MolybdenumBar) lbIsBar = true;
                        if (lnID == ItemEntries.ChromiumBar) lbIsBar = true;

						if (lbIsBar == true)
						{
							lRet = DecrementInventorySlot(i);
							if (lRet != null) 
							{
								CountFreeSlots();
								MarkDirtyDelayed();
								return lRet;
							}
						}

					}


					if (lType == eHopperRequestType.eAnyCraftedItem)
					{
						//we could do an entire check here, but for NOW, just return 'any item', on the basis it was PROBABLY crafted
						lRet = DecrementInventorySlot(i);
						if (lRet != null) 
						{
							CountFreeSlots();
							MarkDirtyDelayed();
							return lRet;
						}	
					}
				}

				if (lType == eHopperRequestType.eAnyCraftedItem)
				{
					if (maItemInventory[i].mType == ItemType.ItemSingle)
					{
						lRet = DecrementInventorySlot(i);
						if (lRet != null) 
						{
							CountFreeSlots();
							MarkDirtyDelayed();
							return lRet;
						}
					}
					if (maItemInventory[i].mType == ItemType.ItemDurability)
					{
						lRet = DecrementInventorySlot(i);
						if (lRet != null) 
						{
							CountFreeSlots();
							MarkDirtyDelayed();
							return lRet;
						}	
					}
					if (maItemInventory[i].mType == ItemType.ItemCharge)
					{
						lRet = DecrementInventorySlot(i);
						if (lRet != null) 
						{
							CountFreeSlots();
							MarkDirtyDelayed();
							return lRet;
						}	
					}
				}

				if (lType == eHopperRequestType.eResearchable)
				{
					// Decomposible cubes types have already been handled in a previous call to GetSpecificCubeRoundRobin (yes this is all really horrible)
					if (maItemInventory[i].mType != ItemType.ItemCubeStack && maItemInventory[i].mnItemID >= 0)
					{
						ItemEntry entry = ItemEntry.mEntries[maItemInventory[i].mnItemID];

						if (entry != null && entry.DecomposeValue > 0)
						{
							lRet = DecrementInventorySlot(i);
							if (lRet != null) 
							{
								CountFreeSlots();
								MarkDirtyDelayed();
								return lRet;
							}	
						}
					}
				}
			}
		}
		
		return lRet;
	}
	// ********************************************************
	public ItemBase RemoveSingleSpecificItemOrCube(eHopperRequestType lType)
	{
		if (lType == eHopperRequestType.eNone) 			return null;
		if (lType == eHopperRequestType.eHighCalorieOnly) return null;
		if (lType == eHopperRequestType.eOreOnly)			return null;
		if (lType == eHopperRequestType.eGarbage)			return null;
		if (lType == eHopperRequestType.eHighCalorieOnly)	return null;
	//	if (lType == eRequestType.eAnyCraftedItem)	return null;//This... should be ok? (or has martijns cube change fucked it?)

		ItemBase lRet = null;

		//we do this even if the mnStorageUsed is 0?

		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;

			//we have no handy way of sub-dividing items into classes or tags
			//For the moment, just search for the most obvious things
			if (lType == eHopperRequestType.eAny)
			{
				lRet = DecrementInventorySlot(i);
				if (lRet != null) 
				{
					CountFreeSlots();
					MarkDirtyDelayed();
					return lRet;
				}
			}
		}

		return lRet;
	}
	// ********************************************************
	//This does NOT return Cubes.
	public ItemBase RemoveSingleSpecificItem(eHopperRequestType lType)
	{
		if (lType == eHopperRequestType.eNone) 			return null;
		if (lType == eHopperRequestType.eHighCalorieOnly) return null;
		if (lType == eHopperRequestType.eOreOnly)			return null;
		if (lType == eHopperRequestType.eGarbage)			return null;
		if (lType == eHopperRequestType.eHighCalorieOnly)	return null;
		//	if (lType == eRequestType.eAnyCraftedItem)	return null;//This... should be ok? (or has martijns cube change fucked it?)
		
		ItemBase lRet = null;
		
		//we do this even if the mnStorageUsed is 0?
		
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;

			if (maItemInventory[i].mType == ItemType.ItemCubeStack) continue;

			//Essentially we want ItemSingle and ItemStack - NOT cubes

			//we have no handy way of sub-dividing items into classes or tags
			//For the moment, just search for the most obvious things
			if (lType == eHopperRequestType.eAny)
			{
				lRet = DecrementInventorySlot(i);
				if (lRet != null) 
				{
					CountFreeSlots();
					MarkDirtyDelayed();
					return lRet;
				}
			}
		}
		
		return lRet;
	}
	// ********************************************************
	ItemBase DecrementInventorySlot(int i)
	{
		ItemBase lRet = null;

		if (maItemInventory[i].mType == ItemType.ItemCubeStack)
		{
			ItemBase item = maItemInventory[i];

			ItemCubeStack lInvStack = item as ItemCubeStack;
			lRet = ItemManager.CloneItem(item);
			(lRet as ItemCubeStack).mnAmount = 1;
			lInvStack.mnAmount--;
			if (lInvStack.mnAmount<=0)
			{
				maItemInventory[i] = null;
			}

			CountFreeSlots();
			MarkDirtyDelayed();

			return lRet;
		}

		//remove item or decrement item stack then return it
		ItemStack lStack = maItemInventory[i] as ItemStack;
		if (lStack != null)
		{
			if ((maItemInventory[i] as ItemStack).mnAmount == 0) return null;//item stack was empty, for whatever reason. Should have been nulled.
			//this seems a little odd, and probably isn't correct
			if ((maItemInventory[i] as ItemStack).mnAmount > 1)
			{
			//	Debug.LogWarning("SH decrementing item stack from " + (maItemInventory[i] as ItemStack).mnAmount);
				(maItemInventory[i] as ItemStack).mnAmount--;
				lRet = ItemManager.CloneItem(maItemInventory[i]);
				(lRet as ItemStack).mnAmount = 1;

				CountFreeSlots();
				MarkDirtyDelayed();

				return lRet;
			}
			else
			{
			//	Debug.LogWarning("SH returning final part of itemstack");
				//Debug.LogWarning("SH returning 1 stack");
				//there's only 1 in the stack, return it
				lRet = maItemInventory[i];
				maItemInventory[i] = null;

				CountFreeSlots();
				MarkDirtyDelayed();

				return lRet;
			}
		}
		else
		{
		//	Debug.LogWarning("SH returning non-itemstack");
			//this didn't convert to an item stack (I think);
			lRet = maItemInventory[i];
			maItemInventory[i] = null;

			CountFreeSlots();
			MarkDirtyDelayed();

			return lRet;
		}

//		Debug.LogWarning("Warning, Storage Hopper was asked to decrement stack and return but did NOT return");
//		return null;
	}
	// ********************************************************
	// Slightly hacky, this returns an entire stack :-)
	public ItemBase RemoveFirstInventoryItem()
	{
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;
			ItemBase lRet = maItemInventory[i];
			maItemInventory[i] = null;
			CountFreeSlots();
			MarkDirtyDelayed();
			if (mnStorageUsed == 0) mLastItemAdded = "Empty";
			RequestImmediateNetworkUpdate();//If this is a server, and a client just removed, immediately push out the change to all client IMMEDIATELY RIGHT NOW
			return lRet;
		}
		return null;
		
	}
	public ItemBase RemoveFirstInventoryItemOrDecrementStack()
	{
		if (mnStorageUsed == 0) Debug.LogError("Attempted to remove item but hopper was empty - are you dumb?");
		for (int i=0;i<mnMaxStorage;i++)
		{
			if (maItemInventory[i] == null) continue;
			ItemBase lRet = ItemManager.CloneItem(maItemInventory[i]);

			if (lRet.mType == ItemType.ItemStack)
			{
				(lRet as ItemStack).mnAmount = 1;
				(maItemInventory[i] as ItemStack).mnAmount--;
				//Debug.LogWarning("SH decremented stack to " + (maItemInventory[i] as ItemStack).mnAmount);
				if ((maItemInventory[i] as ItemStack).mnAmount <=0)
				{
					maItemInventory[i] = null;
				}
			}
			else if (lRet.mType == ItemType.ItemCubeStack)
			{
				(lRet as ItemCubeStack).mnAmount = 1;
				(maItemInventory[i] as ItemCubeStack).mnAmount--;
			//	Debug.LogWarning("SH decremented itemcube stack to " + (maItemInventory[i] as ItemCubeStack).mnAmount);
				if ((maItemInventory[i] as ItemCubeStack).mnAmount <=0)
				{
					maItemInventory[i] = null;
				}
			}
			else
			{
				maItemInventory[i] = null;
			}


			CountFreeSlots();
			MarkDirtyDelayed();
			if (mnStorageUsed == 0) mLastItemAdded = "Empty";
			return lRet;
		}

		return null;
		
	}
	// ********************************************************
	//DOES NOT SUPPORT CUBESTACKS
	public int RemoveInventoryItem(int itemID, int count)
	{
		int amountRemoved = 0;
		
		for (int i = 0; i < mnMaxStorage; i++)
		{
			ItemBase item = maItemInventory[i];
			if (item == null || item.mnItemID != itemID)
				continue;

			if (item.mType == ItemType.ItemCubeStack)
			{
				Debug.LogError("Errror, RemoveInventoryItem does not support cubestacks");
			}

			
			if (item.mType == ItemType.ItemStack)
			{
				ItemStack stack = item as ItemStack;
				
				if (stack.mnAmount <= (count - amountRemoved))
				{
					amountRemoved += stack.mnAmount;
					stack.mnAmount = 0;
					maItemInventory[i] = null;
				}
				else
				{
					stack.mnAmount -= count - amountRemoved;
					amountRemoved = count;
				}
			}
			else
			{
				// non stacking item
				amountRemoved++;
				maItemInventory[i] = null;
			}
				
			if (amountRemoved >= count)
				break;
		}
		
		if (amountRemoved > 0)
		{
			CountFreeSlots();
			MarkDirtyDelayed();
            RequestImmediateNetworkUpdate();
			if (mnStorageUsed == 0) mLastItemAdded = "Empty";
		}
				
		return amountRemoved;
	}	

	// ***************************************************************************************************************************************
	// convenience function for unloading to cargo lifts
	public int UnloadToList (List<ItemBase> cargoList, int amountToExtract)
	{
		// dump our cargo into the given list, until the amount is met
		int amountLeft = amountToExtract;
		for (int i = 0; i < maItemInventory.Length; i++)
		{
			ItemBase item = maItemInventory[i];

            if (item == null) continue;

			int count = ItemManager.GetCurrentStackSize(item);

			if (count > 0)
			{
				if (count > amountLeft)
				{
					// crop old item
					ItemManager.SetItemCount(item, count - amountLeft);

					// create new item
					ItemBase newItem = ItemManager.CloneItem(item);
					ItemManager.SetItemCount(newItem, amountLeft);

					ItemManager.FitCargo(cargoList, newItem);

                    FinaliseHopperChange();

					// we're done
					return amountToExtract;
				}
				else
				{
					// add entire item
					amountLeft -= count;
					ItemManager.FitCargo(cargoList, item);
					maItemInventory[i] = null;
				}
			}
		}


        FinaliseHopperChange();

		return amountToExtract - amountLeft;
	}
    // ***************************************************************************************************************************************
    void FinaliseHopperChange()
    {
        MarkDirtyDelayed();
        RequestImmediateNetworkUpdate();
        CountFreeSlots();
    }
	// ***************************************************************************************************************************************
	public override bool ShouldSave ()
	{
		return true;
	}
	// ***************************************************************************************************************************************
	public override void Write (System.IO.BinaryWriter writer)
	{
		 
		//eventually, the upgrade will cap out at something lower than this
		for (int i=0;i<mnMaxStorage;i++)
		{
			writer.Write((ushort)0); // TODO: Update entity version and remove.
			
		}
		
		writer.Write((int)mPermissions); 

		writer.Write(mbHooverOn);
		int lnDummy = 0;

		writer.Write((byte)0); // partial dummy
		writer.Write((byte)0); // partial dummy
		writer.Write((byte)0); // partial dummy

		writer.Write(mnMaxStorage);
		writer.Write(mrCurrentPower);
		
		writer.Write(mrCurrentTemperature);
		writer.Write(lnDummy);
		writer.Write(lnDummy);
		writer.Write(lnDummy);
		
		for (int i=0;i<mnMaxStorage;i++)
		{
			ItemFile.SerialiseItem(maItemInventory[i],writer);
		}
		 
	}
	// ***************************************************************************************************************************************
	public override void Read (System.IO.BinaryReader reader, int entityVersion)
	{
		//Debug.LogWarning("SH LOADING WITH mnMaxStorage OF "+ mnMaxStorage);

		// I'm not too worried about garbage from this, the chances of any worlds still having any is remote.
		Dictionary<ushort, int> legacyStorageList = null;

		for (int i=0;i<mnMaxStorage;i++)
		{
			ushort cubeType = reader.ReadUInt16();

			if (cubeType != eCubeTypes.NULL)
			{
				// Move into the item inventory.
				if (legacyStorageList == null)
					legacyStorageList = new Dictionary<ushort, int>();

				if (legacyStorageList.ContainsKey(cubeType))
					legacyStorageList[cubeType]++;
				else
					legacyStorageList.Add(cubeType, 1);
			}
			
//			if (maStorage[i] != eCubeTypes.NULL)
//				lastCubeIndex = i;
		}
		
		eHopperPermissions lPermissions = mPermissions; //Store current permissions

		mPermissions = (eHopperPermissions)reader.ReadInt32(); //Read new permissions

        //If the permissions have changed on this network read, display a thing!
		if (!mSegment.mbValidateOnly && NetworkManager.mbClientRunning && FloatingCombatTextManager.instance != null && mbLinkedToGO)
        {
            if (mPermissions != lPermissions && mDistanceToPlayer < 32) //Permissions have changed; player is pretty close
            {
                //Cyan as opposed to green, to show that it's a remote command
                FloatingCombatTextQueue lFQ = FloatingCombatTextManager.instance.QueueText(mnX,mnY+1,mnZ,1.0f,mPermissions.ToString(),Color.cyan,1.5f);
                if (lFQ != null) lFQ.mrStartRadiusRand = 0.25f;
            }
        }

		mbHooverOn = reader.ReadBoolean();

		reader.ReadByte(); // partial dummy
		reader.ReadByte(); // partial dummy
		reader.ReadByte(); // partial dummy

		reader.ReadInt32(); // dummy (mnMaxStorage stored)
		mrCurrentPower = reader.ReadSingle();

		mrCurrentTemperature = reader.ReadSingle();
		//To address endDroppedItemCreationData 
		if (mrCurrentTemperature > 1000) mrCurrentTemperature = 1000;
		if (mrCurrentTemperature <-1000) mrCurrentTemperature =-1000;
		if (float.IsNaN(mrCurrentTemperature)) mrCurrentTemperature = 0;
		if (float.IsInfinity(mrCurrentTemperature)) mrCurrentTemperature = 0;
		reader.ReadInt32(); // dummy
		reader.ReadInt32(); // dummy
		reader.ReadInt32(); // dummy
		
		CountFreeSlots();
		
		if(entityVersion == 0) return;
		
		int lastItemIndex = -1;

		for (int i=0;i<mnMaxStorage;i++)
		{
			maItemInventory[i] = ItemFile.DeserialiseItem(reader);


			
			if (maItemInventory[i] != null)
				lastItemIndex = i;
		}

		CountFreeSlots();

		if (!mSegment.mbValidateOnly)
		{			
			if (lastItemIndex >= 0)
			{
				ItemBase item = maItemInventory[lastItemIndex];

				if (WorldScript.mbHasPlayer)
				{
                    if (item != null && WorldScript.mLocalPlayer.mResearch != null && WorldScript.mLocalPlayer.mResearch.IsKnown(item))
					{
						mLastItemAdded = ItemManager.GetItemName(item);
					}
					else
					{
						mLastItemAdded = PlayerResearch.UnknownMaterial;
					}
				}
			}
		}

		// Support conversion from really old worlds.
		if (legacyStorageList != null)
		{
			foreach(var entry in legacyStorageList) // foreach because dictionary, no I don't care, this is probably never going to get called.
			{
				ushort cubeType = entry.Key;
				int amount = entry.Value;

				ItemBase cubeStack = ItemManager.SpawnCubeStack(cubeType, TerrainData.GetDefaultValue(cubeType), amount);

				AddItem(cubeStack);
			}
		}


		//Assume we came off the network, and things are different
		mbForceHoloUpdate = true;
		mbForceTextUpdate = true;


	}

	// ******************* Network Syncing *************************
	public override bool ShouldNetworkUpdate ()
	{
		return true;
	}
	
	// use defaults for WriteNetworkUpdate and ReadNetworkUpdate
	
	// ***************************************************************************************************************************************
	public void TogglePermissions()
	{
		mPermissions = mPermissions + 1;
		if (mPermissions == eHopperPermissions.eNumPermissions) mPermissions = eHopperPermissions.AddAndRemove;
		MarkDirtyDelayed();
		mbForceTextUpdate = true;
		mbForceHoloUpdate = true;
		RequestImmediateNetworkUpdate();


        FloatingCombatTextQueue lFQ = FloatingCombatTextManager.instance.QueueText(mnX,mnY+1,mnZ,1.0f,mPermissions.ToString(),Color.green,1.5f);
        if (lFQ != null) lFQ.mrStartRadiusRand = 0.25f;

	}

	public void ToggleHoover()
	{
		mbHooverOn = !mbHooverOn;
		MarkDirtyDelayed();
		mbForceTextUpdate = true;
		RequestImmediateNetworkUpdate();
	}

	// ***************************************************************************************************************************************
	public override void OnDelete()
	{
		if (!WorldScript.mbIsServer)
			return;

		System.Random lRand = new System.Random();//temp
		
		for (int i=0;i<mnMaxStorage;i++)
		{
			//DROP ITEMS too, fool!
			if (maItemInventory[i] != null)
			{
				Vector3 lVec = new Vector3(
					(float)lRand.NextDouble() -0.5f,
					(float)lRand.NextDouble() -0.5f,
					(float)lRand.NextDouble() -0.5f);

				if (maItemInventory[i].mType == ItemType.ItemCubeStack)
				{
					Debug.LogWarning ("Dropping " + i +". Count" + (maItemInventory[i] as ItemCubeStack).mnAmount);
				}
				
				ItemManager.instance.DropItem(maItemInventory[i], this.mnX, this.mnY, this.mnZ, lVec);

				maItemInventory[i] = null; // protect against other threads using this item
			}
		}
	}
	// ***************************************************************************************************************************************
	//******************** PowerConsumerInterface **********************
	public float GetRemainingPowerCapacity()
	{
	
		return mrMaxPower - mrCurrentPower;
	}
	
	public float GetMaximumDeliveryRate()
	{

		return 500;
	}
	
	public float GetMaxPower()
	{
	
		return mrMaxPower;
	}

	public bool DeliverPower(float amount)
	{
		if (amount > GetRemainingPowerCapacity()) amount = GetRemainingPowerCapacity();
		
		mrCurrentPower += amount;
		MarkDirtyDelayed();
		return true;
	}
	

	public bool WantsPowerFromEntity(SegmentEntity entity)
	{
		if (mValue == CRYO_HOPPER) return true;
		if (mValue == MOTORISED_HOPPER) return true;
		return false;
	}		

	/// <summary>
	/// Called when the holobase has been opened and it requires this entity to add its
	/// visualisations. If there is no visualisation for an entity return null.
	/// 
	/// To receive updates each frame set the <see cref="HoloMachineEntity.RequiresUpdates"/> flag.
	/// </summary>
	/// <returns>The holobase entity visualisation.</returns>
	/// <param name="holobase">Holobase.</param>
	public override HoloMachineEntity CreateHolobaseEntity(Holobase holobase)
	{
		var creationParameters = new HolobaseEntityCreationParameters(this);

		creationParameters.RequiresUpdates = true;
		creationParameters.AddVisualisation(holobase.mPreviewCube);

		return holobase.CreateHolobaseEntity(creationParameters);
	}

    
	/// <summary>
	/// Called when this entity has added a holobase machine entity with the RequiresUpdates flag
	/// </summary>
	/// <param name="holobase">Holobase.</param>
	/// <param name="holoMachineEntity">Holo machine entity.</param>
	public override void HolobaseUpdate(Holobase holobase, HoloMachineEntity holoMachineEntity)
	{
		if (mnStorageFree == 0)
			holobase.SetColour(holoMachineEntity.VisualisationObjects[0], Color.white);//lMachine.mVisualisationObject.renderer.material.color = Color.red;
		else
			holobase.SetColour(holoMachineEntity.VisualisationObjects[0], Color.green);//lMachine.mVisualisationObject.renderer.material.color = Color.green;
	}	

	//******************** ItemConsumerInterface **********************

	/// <summary>
	/// Attempts to deliver the specified item or cube from the source entity to the consumer
	/// of this interface. If the delivery is successful <c>true</c> will be returned.
	/// </summary>
	/// <returns><c>true</c>, if delivery of item was successful, <c>false</c> otherwise.</returns>
	/// <param name="sourceEntity">Source entity.</param>
	/// <param name="item">Item.</param>
	/// <param name="cubeType">Cube type.</param>
	/// <param name="cubeValue">Cube value.</param>
	/// <param name="sendImmediateNetworkUpdate">Sends an immediate network update.</param>
	public bool TryDeliverItem(StorageUserInterface sourceEntity, ItemBase item, ushort cubeType, ushort cubeValue, bool sendImmediateNetworkUpdate)
	{
		if (mnStorageFree > 0)
		{
			if (item != null)
			{
				AddItem(item);
			}
			else
			{
				AddCube(cubeType, cubeValue);
			}

			if (sendImmediateNetworkUpdate)
			{
				RequestImmediateNetworkUpdate();
			}
			return true;
		}

		return false;
	}


	// ****************************************************************

	public eHopperPermissions GetPermissions()
	{
		return mPermissions;
	}

	public bool IsEmpty()
	{
		return mnStorageUsed <= 0;
	}

	public bool IsFull()
	{
		return mnStorageFree <= 0;
	}

	public bool IsNotEmpty()
	{
		return mnStorageUsed > 0;
	}

	public bool IsNotFull()
	{
		return mnStorageFree > 0;
	}

	public bool InventoryExtractionPermitted
	{
		get { return mbAllowLogistics; }
	}


	public bool TryExtract(InventoryExtractionOptions options, ref InventoryExtractionResults results)
	{
		ItemBase returnedItem;
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		if (!TryExtract(options.RequestType, options.ExemplarItemID, options.ExemplarBlockID, options.ExemplarBlockValue, options.InvertExemplar, 
			options.MinimumAmount, options.MaximumAmount, options.KnownItemsOnly,
			false, false, options.ConvertToItem, out returnedItem, out returnedCubeType, out returnedCubeValue, out returnedAmount))
		{
			// Failed to extract anything
			results.Item = null;
			results.Amount = 0;
			results.Cube = 0;
			results.Value = 0;
			return false;
		}

		results.Cube = returnedCubeType;
		results.Value = returnedCubeValue;
		results.Amount = returnedAmount;
		results.Item = returnedItem;

		return true;
	}

	public bool TryExtract(eHopperRequestType lType, int exemplarItemId, ushort exemplarCubeType, ushort exemplarCubeValue, bool invertExemplar, int minimumAmount, int maximumAmount, bool knownItemsOnly, bool countOnly, bool trashItems, bool convertCubesToItems, out ItemBase returnedItem, out ushort returnedCubeType, out ushort returnedCubeValue, out int returnedAmount)
	{
		// If request type is none, and no exemplar supplied, then return nothing.
		if (lType == eHopperRequestType.eNone && exemplarItemId == -1 && exemplarCubeType == 0)
		{
			returnedItem = null;
			returnedCubeType = 0;
			returnedCubeValue = 0;
			returnedAmount = 0;
			return false;
		}

		int numberFound = 0;
		int numberRemaining = maximumAmount;

		bool matchedItemRemoved = false;
		ItemBase matchedItem = null;

		int itemsToRemove = 0;


		// Now begin searching though the inventory looking for something which matches the filters provided.
		for (int l = 0; l < mnMaxStorage; l++)
		{
			// This offset ensures that each new requests at where the previous one finished, ensuring a more even return of items.
			mnRoundRobinOffset++;
			mnRoundRobinOffset %= mnMaxStorage;

			int i = mnRoundRobinOffset;

			// Grab the item from this inventory slot.
			ItemBase currentItem = maItemInventory[i];

			// No item in this slot.
			if (currentItem == null) 
				continue;

			// If an exemplar item id has been supplied check against it now.
			if (exemplarItemId >= 0)
			{
				// Look for a specific item
				if (currentItem.mnItemID == exemplarItemId)
				{
					// Found the item we want!
					if (invertExemplar)
					{
						// Actually, just found the item we DON'T want.
						continue;
					}
				}
				else
				{
					// Is this not the item we want, unless we have inverted the examplar.
					if (!invertExemplar)
						continue;
				}
			}
			else if (exemplarCubeType > 0)
			{
				// Look for a specific cube
				if (currentItem is ItemCubeStack)
				{
					ItemCubeStack cubeStack = (ItemCubeStack)currentItem;

					if (cubeStack.mCubeType == exemplarCubeType && (cubeStack.mCubeValue == exemplarCubeValue || exemplarCubeValue == ushort.MaxValue))
					{
						// Found the cube we want!
						if (invertExemplar)
						{
							// Actually, just the found cube we DON'T want.
							continue;
						}
					}
					else
					{
						// This not the cube we want, unless we have inverted the examplar.
						if (!invertExemplar)
							continue;
					}
				}
				else
				{
					// This not the cube we want, unless we have inverted the examplar.
					if (!invertExemplar)
						continue;
				}
			}

			// Now check against the filters.
			if (lType != eHopperRequestType.eAny)
			{
				bool matchedRequest = false;

				// We are looking to limit to specific item types
				if (currentItem.mType != ItemType.ItemCubeStack)
				{
					int lnID = currentItem.mnItemID;

					if (lType == eHopperRequestType.eOrganic)
					{
						//Ruined/Pristine parts
						if (lnID >= 4000 && lnID <= 4101) // TODO: Add an IsOrganic tag to the terrain
						{
							matchedRequest = true;	
						}
					}

					if (lType == eHopperRequestType.eBarsOnly)
					{
						bool lbIsBar = false;
						if (lnID == ItemEntries.CopperBar) lbIsBar = true;
						if (lnID == ItemEntries.TinBar) lbIsBar = true;
						if (lnID == ItemEntries.IronBar) lbIsBar = true;
						if (lnID == ItemEntries.LithiumBar) lbIsBar = true;
						if (lnID == ItemEntries.GoldBar) lbIsBar = true;
						if (lnID == ItemEntries.NickelBar) lbIsBar = true;
						if (lnID == ItemEntries.TitaniumBar) lbIsBar = true;

						if (lbIsBar == true)
						{
							matchedRequest = true;
						}
					}

					if (lType == eHopperRequestType.eAnyCraftedItem)
					{
						//we could do an entire check here, but for NOW, just return 'any item', on the basis it was PROBABLY crafted
						matchedRequest = true;
					}

					if (lType == eHopperRequestType.eResearchable)
					{
						ItemEntry entry = ItemEntry.mEntries[lnID];

						if (entry != null && entry.DecomposeValue > 0)
						{
							matchedRequest = true;
						}
					}
				}
				else
				{
					ItemCubeStack lStack = (ItemCubeStack)currentItem;

					if (lType == eHopperRequestType.eResearchable)
					{
						// Decomposible cubes types have already been handled in a previous call to GetSpecificCubeRoundRobin (yes this is all really horrible)
						TerrainDataEntry entry = TerrainData.mEntries[lStack.mCubeType];

						if (entry != null && entry.DecomposeValue > 0)
						{
							matchedRequest = true;
						}
					}

					// And the rest.
					//if (lType == eHopperRequestType.eOrganic) continue;//Unsure if we have any organic blocks yet?

					if (lType == eHopperRequestType.eHighCalorieOnly && CubeHelper.IsHighCalorie(lStack.mCubeType))
						matchedRequest = true;

					if (lType == eHopperRequestType.eOreOnly && CubeHelper.IsSmeltableOre(lStack.mCubeType))
						matchedRequest = true;

					if (lType == eHopperRequestType.eGarbage && CubeHelper.IsGarbage(lStack.mCubeType))
						matchedRequest = true;

					if (lType == eHopperRequestType.eCrystals && lStack.mCubeType == eCubeTypes.OreCrystal)
						matchedRequest = true;

					if (lType == eHopperRequestType.eGems && lStack.mCubeType == eCubeTypes.Crystal)
						matchedRequest = true;

					if (lType == eHopperRequestType.eBioMass && lStack.mCubeType == eCubeTypes.OreBioMass)
						matchedRequest = true;

					if (lType == eHopperRequestType.eSmeltable && CubeHelper.IsIngottableOre(lStack.mCubeType))
						matchedRequest = true;
				}

				if (false == matchedRequest)
				{
					// We failed to match this filter, move onwards.
					continue;
				}
			}

			if (knownItemsOnly)
			{
				// Check research
				if (!WorldScript.mLocalPlayer.mResearch.IsKnown(currentItem))
				{
					// This item is not known to the player.
					continue;
				}
			}

			// If we get to here then this is an item that we want. Yay.
			if (countOnly)
			{
				// If we are only looking for a count then add up the stack size.
				numberFound += ItemManager.GetCurrentStackSize(currentItem);
			}
			else
			{
				// Take out the items required.
				if (matchedItem == null)
				{
					// We have found our first matching item! Set it here so that we can make sure anything else we get out is the same.
					matchedItem = currentItem;
				}
				else
				{
					// This is not the first matching item we have found.
					if (!trashItems)
					{
						// If we are being asked to return the items then everything we take out must be the same type.
						if (matchedItem.mType != currentItem.mType)
						{
							// Different type of item, we cannot get more of this type.
							continue;
						}

						if (matchedItem.mType == ItemType.ItemCubeStack)
						{
							ItemCubeStack matchedCubeStack = (ItemCubeStack)matchedItem;
							ItemCubeStack currentCubeStack = (ItemCubeStack)currentItem;

							if (matchedCubeStack.mCubeType != currentCubeStack.mCubeType)
								continue; // Different cube type.

							if (exemplarCubeValue != ushort.MaxValue) // If this is supplied then we don't give a crap what the value is.
							{
								if (matchedCubeStack.mCubeValue != currentCubeStack.mCubeValue)
									continue; // Different cube value.
							}
						}
						else
						{
							if (matchedItem.mnItemID != currentItem.mnItemID)
							{
								// Not the same item
								continue;
							}
						}
					}
					else
					{
						// We are not being asked to return the items, there is no need for everything to be the same type.
					}
				}

				// Remove this item.
				if (currentItem.mType == ItemType.ItemCubeStack)
				{
					int stackSize = ((ItemCubeStack)currentItem).mnAmount;

					if (stackSize <= numberRemaining)
					{
						// Remove this entire stack.
						numberRemaining -= stackSize;
						numberFound += stackSize;

						if (minimumAmount - numberFound - stackSize > 0)
						{
							// This is a problem. This stack does not meet the minimum requirement for anything
							// to be extracted. We can't remove it from storage right now because we cannot be
							// sure we'll find enough other items.
							mRemoveCache[itemsToRemove] = i;
							itemsToRemove++;
						}
						else
						{
							maItemInventory[i] = null;
						}

						matchedItemRemoved = true;
					}
					else
					{
						// Remove part of this stack.
						((ItemCubeStack)currentItem).mnAmount -= numberRemaining;
						numberFound += numberRemaining;
						numberRemaining = 0;
					}
				}
				else if (currentItem.mType == ItemType.ItemStack)
				{
					int stackSize = ((ItemStack)currentItem).mnAmount;

					if (stackSize <= numberRemaining)
					{
						// Remove this entire stack.
						numberRemaining -= stackSize;
						numberFound += stackSize;

						if (minimumAmount - numberFound - stackSize > 0)
						{
							// This is a problem. This stack does not meet the minimum requirement for anything
							// to be extracted. We can't remove it from storage right now because we cannot be
							// sure we'll find enough other items.
							mRemoveCache[itemsToRemove] = i;
							itemsToRemove++;
						}
						else
						{
							maItemInventory[i] = null;
						}

						matchedItemRemoved = true;
					}
					else
					{
						// Remove part of this stack.
						((ItemStack)currentItem).mnAmount -= numberRemaining;
						numberFound += numberRemaining;
						numberRemaining = 0;
						break;
					}
				}
				else
				{
					if (!trashItems)
					{
						// If we are returning items then we cannot return multiple non-stackable items in a single request.
						returnedItem = matchedItem;
						numberFound = 1;
						numberRemaining = 0;
						maItemInventory[i] = null;
						break;
					}
					else
					{
						// We are trashing items from the hopper.
						numberFound++;
						numberRemaining--;

						if (minimumAmount - numberFound - 1 > 0)
						{
							// This is a problem. This stack does not meet the minimum requirement for anything
							// to be extracted. We can't remove it from storage right now because we cannot be
							// sure we'll find enough other items.
							mRemoveCache[itemsToRemove] = i;
							itemsToRemove++;
						}
						else
						{
							maItemInventory[i] = null;
						}

						matchedItemRemoved = true;
					}
				}

				if (numberRemaining <= 0)
				{
					// We are done.
					break;
				}
			}
		}

		// If we had minimum amounts that stopped us from removing items immediately then do that now.
		for(int j = 0; j < itemsToRemove; j++)
		{
			maItemInventory[mRemoveCache[j]] = null;
		}

		// If we successfully found an item return the details.
		if (matchedItem != null && !countOnly)
		{
			// We found something.
			if (!trashItems)
			{
				// We have been asked to return the items to the caller.
				returnedItem = matchedItem;

				if (matchedItemRemoved)
				{
					if (matchedItem.mType == ItemType.ItemCubeStack)
					{
						ItemCubeStack cubeStack = (ItemCubeStack)matchedItem;

						// Ensure the cube stack has the correct total number found.
						cubeStack.mnAmount = numberFound;
						returnedItem = cubeStack;
						returnedCubeType = cubeStack.mCubeType;
						returnedCubeValue = cubeStack.mCubeValue;
						returnedAmount = numberFound;
					}
					else if (matchedItem.mType == ItemType.ItemStack)
					{
						ItemStack itemStack = (ItemStack)matchedItem;

						// Ensure the item stack has the correct total number found.
						itemStack.mnAmount = numberFound;
						returnedItem = itemStack;
						returnedCubeType = 0;
						returnedCubeValue = 0;
						returnedAmount = numberFound;
					}
					else
					{
						returnedItem = matchedItem;
						returnedCubeType = 0;
						returnedCubeValue = 0;
						returnedAmount = 1;
					}
				}
				else
				{
					if (matchedItem.mType == ItemType.ItemCubeStack)
					{
						ItemCubeStack cubeStack = (ItemCubeStack)matchedItem;

						// We may need to spawn a cube stack here
						if (convertCubesToItems)
						{
							ItemStack itemStack = (ItemStack)ItemManager.CloneItem(cubeStack, numberFound);
						}
						else
						{
							returnedItem = null;
						}

						returnedCubeType = cubeStack.mCubeType;
						returnedCubeValue = cubeStack.mCubeValue;
						returnedAmount = numberFound;
					}
					else if (matchedItem.mType == ItemType.ItemStack)
					{
						ItemStack itemStack = (ItemStack)ItemManager.CloneItem(matchedItem, numberFound);

						// Ensure the item stack has the correct total number found.
						returnedItem = itemStack;
						returnedCubeType = 0;
						returnedCubeValue = 0;
						returnedAmount = numberFound;
					}
					else
					{
						// This should not happen!
						Debug.LogError("Non-stack matched item was not removed from the hopper inventory during extract?");
						returnedItem = matchedItem;
						returnedCubeType = 0;
						returnedCubeValue = 0;
						returnedAmount = 1;
					}
				}
			}
			else
			{
				// We don't need to return the items, just the amount removed from the storage.
				returnedItem = null;
				returnedCubeType = 0;
				returnedCubeValue = 0;
				returnedAmount = numberFound;
			}

			CountFreeSlots();
			MarkDirtyDelayed();

			LogisticsOperation();
			RequestImmediateNetworkUpdate();

			return true;
		}
		else
		{
			// Did not find anything.
			returnedItem = null;
			returnedCubeType = 0;
			returnedCubeValue = 0;
			returnedAmount = 0;
			return false;
		}

		return false;
	}

	public bool TryExtractCubes(StorageUserInterface sourceEntity, ushort cube, ushort value, int amount)
	{
		ItemBase returnedItem;
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		return TryExtract(eHopperRequestType.eAny, -1, cube, value, false, amount, amount, false, false, false, false, out returnedItem, out returnedCubeType, out returnedCubeValue, out returnedAmount);
	}

	public bool TryExtractItems(StorageUserInterface sourceEntity, int itemId, int amount, out ItemBase item)
	{
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		return TryExtract(eHopperRequestType.eAny, itemId, 0, 0, false, amount, amount, false, false, false, true, out item, out returnedCubeType, out returnedCubeValue, out returnedAmount);
	}

	public bool TryExtractItemsOrCubes(StorageUserInterface sourceEntity, int itemId, ushort cube, ushort value, int amount, out ItemBase item)
	{
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		return TryExtract(eHopperRequestType.eAny, itemId, cube, value, false, amount, amount, false, false, false, true, out item, out returnedCubeType, out returnedCubeValue, out returnedAmount);
	}

	public bool TryExtractItemsOrCubes(StorageUserInterface sourceEntity, int itemId, ushort cube, ushort value, int amount)
	{
		ItemBase returnedItem;
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		return TryExtract(eHopperRequestType.eAny, itemId, cube, value, false, amount, amount, false, false, true, false, out returnedItem, out returnedCubeType, out returnedCubeValue, out returnedAmount);
	}

	public bool TryExtractItems(StorageUserInterface sourceEntity, int itemId, int amount)
	{
		ItemBase returnedItem;
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		return TryExtract(eHopperRequestType.eAny, itemId, 0, 0, false, amount, amount, false, false, true, false, out returnedItem, out returnedCubeType, out returnedCubeValue, out returnedAmount);
	}

	public int TryPartialExtractCubes(StorageUserInterface sourceEntity, ushort cube, ushort value, int amount)
	{
		ItemBase returnedItem;
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		bool success = TryExtract(eHopperRequestType.eAny, -1, cube, value, false, amount, amount, false, false, false, false, out returnedItem, out returnedCubeType, out returnedCubeValue, out returnedAmount);

		return returnedAmount;
	}

	public int TryPartialExtractItems(StorageUserInterface sourceEntity, int itemId, int amount, out ItemBase item)
	{
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		TryExtract(eHopperRequestType.eAny, itemId, 0, 0, false, amount, amount, false, false, false, true, out item, out returnedCubeType, out returnedCubeValue, out returnedAmount);

		return returnedAmount;
	}

	public int TryPartialExtractItemsOrCubes(StorageUserInterface sourceEntity, int itemId, ushort cube, ushort value, int amount, out ItemBase item)
	{
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		TryExtract(eHopperRequestType.eAny, itemId, cube, value, false, amount, amount, false, false, false, true, out item, out returnedCubeType, out returnedCubeValue, out returnedAmount);

		return returnedAmount;
	}

	public int TryPartialExtractItemsOrCubes(StorageUserInterface sourceEntity, int itemId, ushort cube, ushort value, int amount)
	{
		ItemBase returnedItem;
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		TryExtract(eHopperRequestType.eAny, itemId, cube, value, false, amount, amount, false, false, false, false, out returnedItem, out returnedCubeType, out returnedCubeValue, out returnedAmount);

		return returnedAmount;
	}


	public int TryPartialExtractItems(StorageUserInterface sourceEntity, int itemId, int amount)
	{
		ItemBase returnedItem;
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		TryExtract(eHopperRequestType.eAny, itemId, 0, 0, false, amount, amount, false, false, true, false, out returnedItem, out returnedCubeType, out returnedCubeValue, out returnedAmount);

		return returnedAmount;
	}


	public bool TryExtractAny(StorageUserInterface sourceEntity, int amount, out ItemBase item)
	{
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		return TryExtract(eHopperRequestType.eAny, -1, 0, 0, false, amount, amount, false, false, false, true, out item, out returnedCubeType, out returnedCubeValue, out returnedAmount);
	}


	public bool TryInsert(InventoryInsertionOptions options, ref InventoryInsertionResults results)
	{
		// Check we have not run out of space.
		if (mnStorageFree <= 0)
		{
			return false;
		}

		if (options.Item != null)
		{
			// Get the number of items represented by this item.
			int totalItemCount = ItemManager.GetCurrentStackSize(options.Item);

			int insertItemCount = totalItemCount;

			ItemBase item;
			if (totalItemCount > mnStorageFree)
			{
				if (!options.AllowPartialInsertion)
				{
					// Partial insertion is not allowed.
					return false;
				}

				insertItemCount = mnStorageFree;
				item = ItemManager.CloneItem(options.Item, insertItemCount);
			}
			else
			{
				item = options.Item;
			}

			if (false == AddItem(item))
			{
				// This should not happen. Race condition?
				return false;
			}

			// Only create results if required.
			if (results == null)
				results = new InventoryInsertionResults();
			
			results.AmountInserted = insertItemCount;
			results.AmountRemaining = totalItemCount - insertItemCount;
			return false;
		}
		else
		{
			if (options.Amount == 1)
			{
				AddCube(options.Cube, options.Value);

				if (results == null)
					results = new InventoryInsertionResults();
				
				results.AmountInserted = 1;
				return true;
			}
			else
			{
				// TODO: More efficient way of inserting multiple items.
				if (options.Amount > mnStorageFree && !options.AllowPartialInsertion)
				{
					return false;
				}

				int remaining = options.Amount;

				while(remaining > 0 && mnStorageFree > 0)
				{
					AddCube(options.Cube, options.Value);
					remaining--;
				}

				if (results == null)
					results = new InventoryInsertionResults();
				
				results.AmountInserted = options.Amount - remaining;
				results.AmountRemaining = remaining;
				return true;
			}
		}

		//		if (maAttachedHoppers[0].mnStorageFree <=0)
		//		{
		//			Debug.LogError("Derp, how did a Quarry pick a full hopper to empty into?");
		//		}
		//		else
		//		{
		//			maAttachedHoppers[0].AddCube(mCarryCube,mCarryValue);
		//		}
	}

	public bool TryInsert(StorageUserInterface sourceEntity, ItemBase item)
	{
		return AddItem(item);
	}

	public bool TryInsert(StorageUserInterface sourceEntity, ushort cube, ushort value, int amount)
	{
		if (amount == 1)
		{
			AddCube(cube, value);
			return true;
		}

		if (mnStorageFree < amount)
			return false;

		int remaining = amount;

		while(remaining > 0 && mnStorageFree > 0)
		{
			AddCube(cube, value);
			remaining--;
		}

		return true;
	}

	public int TryPartialInsert(StorageUserInterface sourceEntity, ItemBase item)
	{
		int totalItemCount = ItemManager.GetCurrentStackSize(item);

		int insertItemCount = totalItemCount;

		ItemBase copyItem;
		if (totalItemCount > mnStorageFree)
		{
			insertItemCount = mnStorageFree;
			copyItem = ItemManager.CloneItem(item, insertItemCount);
		}
		else
		{
			copyItem = item;
		}

		if (false == AddItem(copyItem))
		{
			// This should not happen. Race condition?
			return 0;
		}

		return insertItemCount;
	}

	public int TryPartialInsert(StorageUserInterface sourceEntity, ushort cube, ushort value, int amount)
	{
		int remaining = amount;

		while(remaining > 0 && mnStorageFree > 0)
		{
			AddCube(cube, value);
			remaining--;
		}

		return amount - remaining;
	}

	public int CountItems(InventoryExtractionOptions options)
	{
		// Use the full extract function to count using all provided options.
		ItemBase returnedItem;
		ushort returnedCubeType;
		ushort returnedCubeValue;
		int returnedAmount;

		if (!TryExtract(options.RequestType, options.ExemplarItemID, options.ExemplarBlockID, options.ExemplarBlockValue, options.InvertExemplar, 
			options.MinimumAmount, options.MaximumAmount, options.KnownItemsOnly,
			true, false, false, out returnedItem, out returnedCubeType, out returnedCubeValue, out returnedAmount))
		{
			// Failed to extract anything
			return 0;
		}

		return returnedAmount;
	}

	public int CountItems(int itemId)
	{
		return CountHowManyOfItem(itemId);
	}

	public int CountItems(int itemId, ushort cube, ushort value)
	{
		if (itemId >= 0)
		{
			return CountHowManyOfItem(itemId);
		}
		else
		{
			return CountHowManyOfType(cube, value);
		}
	}

	public int CountCubes(ushort cube, ushort value)
	{
		return CountHowManyOfType(cube, value);
	}

	public int TotalCapacity
	{
		get { return mnMaxStorage; }
	}

	public int UsedCapacity
	{ 
		get { return mnStorageUsed; }
	}

	public int RemainingCapacity
	{
		get { return mnStorageFree; }
	}

	float mRetakeDebounce;

	public override void Selected()
	{
		// When a hopper is selected ensure the initial debounce time is 0.
		mRetakeDebounce = 0.0f;
	}


}