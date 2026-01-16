import {
	addOrUpdateCmdLineArgs,
	addOrUpdateNodeLogs,
	addOrUpdateNodeFlags,
	addOrUpdateNodeVersion,
	addOrUpdateSyncStages,
	LogFile,
	DataBase,
	addOrUpdateDBs,
	DataBaseTable,
	addOrUpdateDBTable,
	updateNodesInfoInCurrentSession,
	Reorg,
	addOrUpdateReorg
} from "../app/store/appSlice";
import {
	fetchBackendUrl,
	fetchBootnodes,
	fetchCmdLineArgs,
	fetchDB,
	fetchDBList,
	fetchFlags,
	fetchHeaders,
	fetchLogFilesList,
	fetchNetworkSpeed,
	fetchNodeInfo,
	fetchPeers,
	fetchReorgs,
	fetchSession,
	fetchSnapshotFilesList,
	fetchSnapshotSync,
	fetchSyncStages,
	fetchVersion,
	logListUrl
} from "./APIHandler";
import { nodeInfoFromJson, nodeInfoFromJsonLocal } from "../helpers/nodeInfoFromJson";
import { flagsFromJson } from "../helpers/flagsFromJson";
import { versionFromJson } from "../helpers/versionFromJson";
import { syncStagesFromJson } from "../helpers/syncStagesFromJson";
import { store } from "../app/store/store";
import { NodeInfo } from "../entities";
import { peerFromJson } from "../helpers/peerFromJson";
import { Peer, addOrUpdatePeer, addOrUpdateBootnodes } from "../app/store/networkSlice";
import { snapshotDownloadStatusFromJson, snapshotIndexStatusFromJson } from "../helpers/snapshoSyncFromJson";
import {
	SnapshotDownloadStatus,
	SnapshotIndexingStatus,
	SyncStage,
	SyncStages,
	setNodeSyncStages,
	setSnapshotDownloadStatus,
	setSnapshotFilesList,
	setSnapshotIndexStatus
} from "../app/store/syncStagesSlice";
import { addOrUpdateHeaders } from "../app/store/headersSlice";
import { headersFromJson } from "../helpers/headersFromJson";
import { NetworkSpeed, addOrUpdateNetworkSpeed } from "../app/store/systemInfoSlice";
import { NodeConnectionType, setBackendAddress, setNodeConnectionType } from "../app/store/connectionSlice";

const getNodeId = (): string => {
	return store.getState().app.activeNodeId;
};

export const getBackendUrl = () => {
	fetchBackendUrl()
		.then((response) => {
			console.log("Backend address: ", response?.address);
			if (response.address != undefined) {
				store.dispatch(setNodeConnectionType(NodeConnectionType.Local));
				store.dispatch(setBackendAddress(response.address));
				console.log("Backend address: ", response.address);
			} else {
				store.dispatch(setNodeConnectionType(NodeConnectionType.Unknown));
			}
		})
		.catch((error) => {
			store.dispatch(setNodeConnectionType(NodeConnectionType.Remote));
		});
};

const getCurrentSnapshotDownloadStatus = (): SnapshotDownloadStatus | undefined => {
	let nodeId = getNodeId();

	for (let snapshotStatus of store.getState().syncStages.snapshotDownloadStatus) {
		if (snapshotStatus.nodeId === nodeId) {
			return snapshotStatus.downloadStatus;
		}
	}

	return undefined;
};

export const getSession = () => {
	fetchSession().then((response) => {
		let nodes: NodeInfo[] = [];
		if (response?.nodes !== null) {
			response.nodes.forEach((node: any) => {
				nodes.push(nodeInfoFromJson(node));
			});
		}

		store.dispatch(updateNodesInfoInCurrentSession(nodes));
	});
};

export const getNodeInfo = () => {
	fetchNodeInfo().then((response) => {
		let nodes: NodeInfo[] = [];
		if (response !== null && response.nodes_info.length > 0) {
			nodes.push(nodeInfoFromJsonLocal(response.nodes_info[0]));
		}

		store.dispatch(updateNodesInfoInCurrentSession(nodes));
	});
};

export const getNodeVersion = () => {
	fetchVersion().then((response: any) => {
		let nodeVersion = versionFromJson(response);
		store.dispatch(addOrUpdateNodeVersion({ nodeId: getNodeId(), version: nodeVersion }));
	});
};

export const getNodeFlags = () => {
	fetchFlags().then((response: any) => {
		let flags = flagsFromJson(response);
		store.dispatch(addOrUpdateNodeFlags({ nodeId: getNodeId(), flags: flags }));
	});
};

export const getNodeCmdLineArgs = () => {
	fetchCmdLineArgs().then((response: any) => {
		store.dispatch(addOrUpdateCmdLineArgs({ nodeId: getNodeId(), args: response }));
	});
};

export const getSyncStages = () => {
	fetchSyncStages().then((response) => {
		store.dispatch(addOrUpdateSyncStages({ nodeId: getNodeId(), syncStages: syncStagesFromJson(response) }));
	});
};

export const getLogs = () => {
	fetchLogFilesList().then((response) => {
		let logFiles: LogFile[] = [];
		response.forEach((logFile: any) => {
			if (logFile.name.endsWith(".log")) {
				const baseurl = logListUrl();
				const url = `${baseurl}/${logFile.name}`;
				logFiles.push({ name: logFile.name, size: logFile.size, url: url, selected: false });
			}
		});
		store.dispatch(addOrUpdateNodeLogs({ nodeId: getNodeId(), logFiles: logFiles }));
	});
};

export const getDBsList = () => {
	fetchDBList().then((dataBasePathList) => {
		let dataBases: DataBase[] = [];
		dataBasePathList.forEach((dataBasePath: any) => {
			dataBases.push({ path: dataBasePath, tables: [], keysCount: 0, size: 0 });
		});

		store.dispatch(addOrUpdateDBs({ nodeId: getNodeId(), dbs: dataBases }));
	});
};

export const getDB = (dbPath: string) => {
	fetchDB(dbPath).then((tables) => {
		let keysCount = 0;
		let size = 0;
		let dbTables: DataBaseTable[] = [];
		tables.forEach((table: any) => {
			keysCount += table.count;
			size += table.size;
			dbTables.push({
				name: table.name,
				count: table.count,
				size: table.size
			});
		});
		store.dispatch(addOrUpdateDBTable({ nodeId: getNodeId(), path: dbPath, tables: dbTables, keysCount: keysCount, size: size }));
	});
};

export const getReorgs = () => {
	fetchReorgs().then((response) => {
		let wb: number[] = [];
		if (response.WrongBlocks !== null) {
			response.WrongBlocks.forEach((block: number) => {
				wb.push(block);
			});
		}

		let reorg: Reorg = {
			nodeId: getNodeId(),
			totalBlocks: response.TotalScanned,
			wrongBlocks: wb,
			timeTook: response.TimeTook
		};

		store.dispatch(addOrUpdateReorg(reorg));
	});
};

//TODO: think about refactore bootnodes / peers relationship
export const getPeers = () => {
	const activeNodeId = getNodeId();
	const nodeIdx = store.getState().network.bootnodes.findIndex((bootnode) => bootnode.nodeId === activeNodeId);
	let bootnodes: string[] = [];

	if (nodeIdx !== -1) {
		bootnodes = store.getState().network.bootnodes[nodeIdx].bootnodes;
	}

	fetchPeers().then((peers) => {
		peers.forEach((val: any) => {
			let peer: Peer = peerFromJson(val, bootnodes);
			store.dispatch(addOrUpdatePeer({ peer: peer, nodeId: getNodeId() }));
		});
	});
};

export const getBootnodes = () => {
	fetchBootnodes().then((bootnodes) => {
		store.dispatch(addOrUpdateBootnodes({ nodeId: getNodeId(), bootnodes: bootnodes }));
	});
};

export const getSnapshotDownloadStatus = () => {
	fetchSnapshotSync().then((response) => {
		if (response) {
			let currDStats = getCurrentSnapshotDownloadStatus();
			let segCount = 0;
			if (currDStats) {
				currDStats.segments.forEach((seg) => {
					if (!seg.name.includes("beaconblocks")) {
						segCount++;
					}
				});
			}
			let indexingStatus: SnapshotIndexingStatus = snapshotIndexStatusFromJson(response.snapshotIndexing, segCount);
			let downloadStatus: SnapshotDownloadStatus = snapshotDownloadStatusFromJson(response.snapshotDownload, currDStats, indexingStatus);

			//not correct stages, need to remove -1 fail on execution
			let cs = response.syncStages.currentStage;
			if (cs === 0) cs = 1;

			let stgs: SyncStage[] = [];
			if (response.syncStages.stagesList != null) {
				response.syncStages.stagesList.forEach((stage: any) => {
					let substage = false;
					if (stage === "Snapshots") substage = true;

					stgs.push({ name: stage, subStage: substage });
				});
			}

			if (stgs.length != 0) {
				let syncStages: SyncStages = { stages: stgs, currentStage: cs };

				store.dispatch(setNodeSyncStages({ nodeId: getNodeId(), stages: syncStages }));
			}
			store.dispatch(setSnapshotIndexStatus({ nodeId: getNodeId(), indexStatus: indexingStatus }));
			store.dispatch(setSnapshotDownloadStatus({ nodeId: getNodeId(), downloadStatus: downloadStatus }));
		}
	});
};

export const getSnapshotFilesList = () => {
	fetchSnapshotFilesList().then((response) => {
		store.dispatch(setSnapshotFilesList({ nodeId: getNodeId(), files: response.files }));
	});
};

export const getHeaders = () => {
	fetchHeaders().then((response) => {
		const hdrs = headersFromJson(response);
		store.dispatch(addOrUpdateHeaders({ nodeId: getNodeId(), headers: hdrs }));
	});
};

export const getNetworkSpeed = () => {
	fetchNetworkSpeed().then((response) => {
		let networkSpeed: NetworkSpeed = {
			latency: response?.latency || 0,
			downloadSpeed: response?.downloadSpeed || 0,
			uploadSpeed: response?.uploadSpeed || 0
		};

		store.dispatch(addOrUpdateNetworkSpeed({ nodeId: getNodeId(), networkSpeed: networkSpeed }));
	});
};