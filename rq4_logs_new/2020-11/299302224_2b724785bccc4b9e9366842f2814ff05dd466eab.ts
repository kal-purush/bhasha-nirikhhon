// package: test
// file: test.proto

/* tslint:disable */
/* eslint-disable */

import * as grpc from "grpc";
import * as test_pb from "./test_pb";

interface ITestServiceService extends grpc.ServiceDefinition<grpc.UntypedServiceImplementation> {
    test: ITestServiceService_Itest;
}

interface ITestServiceService_Itest extends grpc.MethodDefinition<test_pb.TestMessage, test_pb.TestMessage> {
    path: "/test.TestService/test";
    requestStream: false;
    responseStream: true;
    requestSerialize: grpc.serialize<test_pb.TestMessage>;
    requestDeserialize: grpc.deserialize<test_pb.TestMessage>;
    responseSerialize: grpc.serialize<test_pb.TestMessage>;
    responseDeserialize: grpc.deserialize<test_pb.TestMessage>;
}

export const TestServiceService: ITestServiceService;

export interface ITestServiceServer {
    test: grpc.handleServerStreamingCall<test_pb.TestMessage, test_pb.TestMessage>;
}

export interface ITestServiceClient {
    test(request: test_pb.TestMessage, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<test_pb.TestMessage>;
    test(request: test_pb.TestMessage, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<test_pb.TestMessage>;
}

export class TestServiceClient extends grpc.Client implements ITestServiceClient {
    constructor(address: string, credentials: grpc.ChannelCredentials, options?: object);
    public test(request: test_pb.TestMessage, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<test_pb.TestMessage>;
    public test(request: test_pb.TestMessage, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<test_pb.TestMessage>;
}