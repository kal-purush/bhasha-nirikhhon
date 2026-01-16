///<reference path='../interfaces/IDataChannel.ts'/>
///<reference path='../interfaces/IDataStorageConfiguration.ts'/>
///<reference path='../interfaces/INode.ts'/>

import { SynchronizedTree } from '../structure/SynchronizedTree';
import { DataStorage } from '../dataChannels/DataStorage';
import { NedbDatabaseDataChannel } from '../dataChannels/NedbDatabaseDataChannel';
import { Constants } from '../interfaces/Constants';

function createDataStorage(): DataStorage {
    return new DataStorage({
        channels: {
            _nodes: {
                system: true
            }
        }
    }, (): IDataChannel => {
        return new NedbDatabaseDataChannel();
    });
}

describe('Synchronized Tree', () => {
    it('should update contents based on new tree item added', (done) => {
        var dataStorage = createDataStorage();
        var tree = new SynchronizedTree(dataStorage);
        tree.setCacheUpdateInterval(0); // to update it for each request
        var nodesChannel = dataStorage.getChannel(Constants.DATA_CHANNEL_NODES);
        expect(nodesChannel).toBeTruthy();
        var node: INode = <any> {
            id: 'abc',
            parentId: null,
            name: 'node1',
            description: 'description'
        };
        nodesChannel.create(node, (error: Error) => {
            expect(error).toBeFalsy();
            tree.getRootNode((error: Error, rootNode: INode) => {
                expect(error).toBeFalsy();
                expect(rootNode).toBeTruthy();
                expect(rootNode.description).toBe(node.description);
                expect(rootNode.children.length).toBe(0);

                var child1: INode = <any> {
                    id: 'def',
                    parentId: rootNode.id,
                    name: 'node2',
                    description: 'description2'
                };

                var child2: INode = <any> {
                    id: 'ghi',
                    parentId: 'def',
                    name: 'node3',
                    description: 'description3'
                };

                nodesChannel.createMany([child1, child2], (error: Error) => {
                    expect(error).toBeFalsy();
                    tree.getRootNode((error: Error, rootNode: INode) => {
                        expect(rootNode.children.length).toBe(1);
                        var child = rootNode.children[0];
                        expect(child.name).toBe(child1.name);
                        expect(child.children.length).toBe(1);
                        child = child.children[0];
                        expect(child.name).toBe(child2.name);
                        nodesChannel.remove(child2.id, (error: Error) => {
                            expect(error).toBeFalsy();
                            tree.getRootNode((error: Error, rootNode: INode) => {
                                expect(error).toBeFalsy();
                                expect(rootNode.children.length).toBe(1);
                                expect(rootNode.children[0].children.length).toBe(0);
                                done();
                            });
                        });
                    });
                });
            });
        });
    });

    it('should give concrete node by its id', (done) => {
        var node1: INode = <any> {
            id: 'abc',
            parentId: null,
            name: 'node1',
            description: 'description'
        };
        var node2: INode = <any> {
            id: 'def',
            parentId: node1.id,
            name: 'node2',
            description: 'description2'
        };
        var dataStorage = createDataStorage();
        var tree = new SynchronizedTree(dataStorage);
        tree.setCacheUpdateInterval(0); // to update it for each request
        var nodesChannel = dataStorage.getChannel(Constants.DATA_CHANNEL_NODES);
        expect(nodesChannel).toBeTruthy();
        nodesChannel.createMany([node1, node2], (error: Error) => {
            expect(error).toBeFalsy();
            tree.getNodeById(node2.id, (error: Error, node: INode) => {
                expect(error).toBeFalsy();
                expect(node.name).toBe(node2.name);
                done();
            });
        });
    });
});