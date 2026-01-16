from __future__ import print_function

import argparse
import json
import os
import sys
import time
import traceback
from threading import Thread
from concurrent import futures
#from multiprocessing import Array, Manager, Value
import logging
import gunicorn.app.base
from flask import Flask, Response, json, make_response, request
import prometheus_client
from prometheus_client import CollectorRegistry, Summary, multiprocess, Histogram

from ExternalServiceExecutor import init_REST, init_gRPC, run_external_service
from InternalServiceExecutor import InternalServiceExecutor 

import mub_pb2_grpc as pb2_grpc
import mub_pb2 as pb2
import grpc


# Configuration of global variables

jaeger_headers_list = [
    'x-request-id',
    'x-b3-traceid',
    'x-b3-spanid',
    'x-b3-parentspanid',
    'x-b3-sampled',
    'x-b3-flags',
    'x-datadog-trace-id',
    'x-datadog-parent-id',
    'x-datadog-sampling-priority',
    'x-ot-span-context',
    'grpc-trace-bin',
    'traceparent',
    'x-cloud-trace-context',
]

# Flask APP
app = Flask(__name__)
ID = os.environ["APP"]
ZONE = os.environ["ZONE"]  # Pod Zone
K8S_APP = os.environ["K8S_APP"]  # K8s label app
PN = os.environ["PN"] # Number of processes
TN = os.environ["TN"] # Number of thread per process
traceEscapeString = "__"

#globalDict=Manager().dict()
globalDict=dict()
def read_config_files():
    res = dict()
    with open('HealthcareWorkspace/workmodel.json') as f:
        workmodel = json.load(f)
        # shrink workmodel
        for service in workmodel:
            app.logger.info(f'service: {service}')
            if service==ID:
                res[service]=workmodel[service]
            else:
                res[service]={"url":workmodel[service]["url"],"path":workmodel[service]["path"]}
    return res
globalDict['work_model'] = read_config_files()    # must be shared among processes for hot update

# default behaviour
work_model_config = globalDict['work_model'][ID]
service_mesh_config = work_model_config['external_services'] 
internal_service_config = work_model_config['internal_service']

if "request_method" in globalDict['work_model'][ID].keys():
    request_method = globalDict['work_model'][ID]["request_method"].lower()
else:
    request_method = "rest"

########################### PROMETHEUS METRICS
registry = CollectorRegistry()
multiprocess.MultiProcessCollector(registry)

CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')
RESPONSE_SIZE = Summary('mub_response_size', 'Response size',
                        ['zone', 'app_name', 'method', 'endpoint', 'from', 'kubernetes_service'], registry=registry
                        )

INTERNAL_PROCESSING = Summary('mub_internal_processing_latency_milliseconds', 'Latency of internal service',
                           ['zone', 'app_name', 'method', 'endpoint'],registry=registry
                           )
EXTERNAL_PROCESSING = Summary('mub_external_processing_latency_milliseconds', 'Latency of external services',
                           ['zone', 'app_name', 'method', 'endpoint'], registry=registry
                           )
REQUEST_PROCESSING = Summary('mub_request_processing_latency_milliseconds', 'Request latency including external and internal service',
                           ['zone', 'app_name', 'method', 'endpoint', 'from', 'kubernetes_service'],registry=registry
                           )

buckets=[0.5, 1, 10, 100 ,1000, 10000, float("inf")] 
INTERNAL_PROCESSING_BUCKET = Histogram('mub_internal_processing_latency_milliseconds_bucket', 'Latency of internal service',
                           ['zone', 'app_name', 'method', 'endpoint'],registry=registry,buckets=buckets
                           )
EXTERNAL_PROCESSING_BUCKET = Histogram('mub_external_processing_latency_milliseconds_bucket', 'Latency of external services',
                           ['zone', 'app_name', 'method', 'endpoint'], registry=registry,buckets=buckets
                           )
REQUEST_PROCESSING_BUCKET = Histogram('mub_request_processing_latency_milliseconds_bucket', 'Request latency including external and internal service',
                           ['zone', 'app_name', 'method', 'endpoint', 'from', 'kubernetes_service'],registry=registry,buckets=buckets
)

internal_service_executer = InternalServiceExecutor(internal_service_config)

@app.route(f"{globalDict['work_model'][ID]['path']}", methods=['GET','POST'])
def start_worker():
    global work_model_config 
    global service_mesh_config 
    global internal_service_config 
    global InternalServiceExecutor
    try:
        start_request_processing = time.time()
        app.logger.info('Request Received')
        
        query_string = request.query_string.decode()
        behaviour_id = request.args.get('bid', default = 'default', type = str)
        # update internal service behaviour
        if behaviour_id != 'default' and "alternative_behaviors" in work_model_config.keys():
                if behaviour_id in work_model_config['alternative_behaviors'].keys():
                    if "internal_services" in work_model_config['alternative_behaviors'][behaviour_id].keys():
                        internal_service_config = work_model_config['alternative_behaviors'][behaviour_id]['internal_service']

        # trace context propagation
        jaeger_headers = dict()
        for jhdr in jaeger_headers_list:
            val = request.headers.get(jhdr)
            if val is not None:
                jaeger_headers[jhdr] = val

        # if POST check the presence of a trace
        trace=dict()
        if request.method == 'POST':
            trace = request.json
            # sanity_check
            assert len(trace.keys())==1, 'bad trace format'
            assert ID == list(trace)[0].split(traceEscapeString)[0], "bad trace format, ID"
            trace[ID] = trace[list(trace)[0]] # We insert 1 more key "s0": [value] 
            
        if len(trace)>0:
        # trace-driven request
            n_groups = len(trace[ID])
            service_mesh_config = list()
            for i in range(0,n_groups):
                group = trace[ID][i]
                group_dict = dict()
                group_dict['seq_len'] = len(group)
                group_dict['services'] = list(group.keys())
                service_mesh_config.append(group_dict)
        else:
            # update external service behaviour
            if behaviour_id != 'default' and "alternative_behaviors" in work_model_config.keys():
                if behaviour_id in work_model_config['alternative_behaviors'].keys():
                    if "external_services" in work_model_config['alternative_behaviors'][behaviour_id].keys():
                        service_mesh_config = work_model_config['alternative_behaviors'][behaviour_id]['external_services']

        # Execute the internal service
        app.logger.info("*************** INTERNAL SERVICE STARTED ***************")
        start_local_processing = time.time()
        body = internal_service_executer.run_internal_service() # TODO: This implementation is not supported for trace driven
        local_processing_latency = time.time() - start_local_processing
        INTERNAL_PROCESSING.labels(ZONE, K8S_APP, request.method, request.path).observe(local_processing_latency*1000)
        INTERNAL_PROCESSING_BUCKET.labels(ZONE, K8S_APP, request.method, request.path).observe(local_processing_latency*1000)
        RESPONSE_SIZE.labels(ZONE, K8S_APP, request.method, request.path, request.remote_addr, ID).observe(len(body))
        app.logger.info("len(body): %d" % len(body))
        app.logger.info("############### INTERNAL SERVICE FINISHED! ###############")
        # Execute the external services
        start_external_request_processing = time.time()
        app.logger.info("*************** EXTERNAL SERVICES STARTED ***************")
        
        if len(service_mesh_config) > 0:
            if len(trace)>0:
                service_error_dict = run_external_service(service_mesh_config,globalDict['work_model'],query_string,trace[ID],app, jaeger_headers)
            else:
                service_error_dict = run_external_service(service_mesh_config,globalDict['work_model'],query_string,dict(),app, jaeger_headers)
            if len(service_error_dict):
                app.logger.error(service_error_dict)
                app.logger.error("Error in request external services")
                app.logger.error(service_error_dict)
                return make_response(json.dumps({"message": "Error in external services request"}), 500)
        app.logger.info("############### EXTERNAL SERVICES FINISHED! ###############")

        response = make_response(body)
        response.mimetype = "text/plain"
        EXTERNAL_PROCESSING.labels(ZONE, K8S_APP, request.method, request.path).observe((time.time() - start_external_request_processing)*1000)
        EXTERNAL_PROCESSING_BUCKET.labels(ZONE, K8S_APP, request.method, request.path).observe((time.time() - start_external_request_processing)*1000)
        
        REQUEST_PROCESSING.labels(ZONE, K8S_APP, request.method, request.path, request.remote_addr, ID).observe((time.time() - start_request_processing)*1000)
        REQUEST_PROCESSING_BUCKET.labels(ZONE, K8S_APP, request.method, request.path, request.remote_addr, ID).observe((time.time() - start_request_processing)*1000)

        # Add trace context propagation headers to the response
        response.headers.update(jaeger_headers)

        return response
    except Exception as err:
        app.logger.error("Error in start_worker", err)
        # app.logger.error(traceback.format_exc())
        return json.dumps({"message": "Error"}), 500

# Prometheus
@app.route('/metrics')
def metrics():
    return Response(prometheus_client.generate_latest(registry), mimetype=CONTENT_TYPE_LATEST)

# Custom Gunicorn application: https://docs.gunicorn.org/en/stable/custom.html
class HttpServer(gunicorn.app.base.BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        config = {key: value for key, value in self.options.items()
                  if key in self.cfg.settings and value is not None}
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

# GRPC (no multi-process)
gRPC_port = 51313
class gRPCThread(Thread, pb2_grpc.MicroServiceServicer):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=int(TN)))

    def __init__(self):
        Thread.__init__(self)

    def GetMicroServiceResponse(self, req, context):
        try:
            start_request_processing = time.time()
            app.logger.info.info('Request Received')
            message = req.message
            remote_address = context.peer().split(":")[1]
            app.logger.info(f'I am service: {ID} and I received this message: --> "{message}"')

            # Execute the internal service
            app.logger.info("*************** INTERNAL SERVICE STARTED ***************")
            start_local_processing = time.time()
            body = run_internal_service(work_model_config["internal_service"])
            local_processing_latency = time.time() - start_local_processing
            INTERNAL_PROCESSING.labels(ZONE, K8S_APP, "grpc", "grpc").observe(local_processing_latency*1000)
            RESPONSE_SIZE.labels(ZONE, K8S_APP, "grpc", "grpc", remote_address, ID).observe(len(body))
            app.logger.info("len(body): %d" % len(body))
            app.logger.info("############### INTERNAL SERVICE FINISHED! ###############")

            # Execute the external services
            app.logger.info("*************** EXTERNAL SERVICES STARTED ***************")
            start_external_request_processing = time.time()
            if len(service_mesh_config) > 0:
                service_error_dict = run_external_service(service_mesh_config, globalDict['work_model'])
                if len(service_error_dict):
                    app.logger.error(service_error_dict)
                    app.logger.error("Error in request external services")
                    app.logger.error(service_error_dict)
                    result = {'text': f"Error in external services request", 'status_code': False}
                    return pb2.MessageResponse(**result)
            app.logger.info("############### EXTERNAL SERVICES FINISHED! ###############")

            result = {'text': body, 'status_code': True}
            EXTERNAL_PROCESSING.labels(ZONE, K8S_APP, "grpc", "grpc").observe((time.time() - start_external_request_processing)*1000)
            REQUEST_PROCESSING.labels(ZONE, K8S_APP, "grpc", "grpc", remote_address, ID).observe(
                (time.time() - start_request_processing)*1000)
            return pb2.MessageResponse(**result)
        except Exception as err:
            app.logger.error("Error: in GetMicroServiceResponse,", err)
            result = {'text': f"Error: in GetMicroServiceResponse, {str(err)}", 'status_code': False}
            return pb2.MessageResponse(**result)

    def run(self):
        pb2_grpc.add_MicroServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f'[::]:{gRPC_port}')
        self.server.start()

if __name__ == '__main__':
    # 初始化interal function
    if request_method == "rest":
        init_REST(app)
        # Start Gunicorn HTTP REST Server (multi-process)
        options_gunicorn = {
            'bind': '%s:%s' % ('0.0.0.0', 8080),
            'workers': PN,
            'config': "/app/gunicorn.conf.py",
            'threads':TN
        }
        HttpServer(app, options_gunicorn).run()
    elif request_method == "grpc":
        work_model_config = globalDict['work_model'][ID]
        service_mesh_config = work_model_config['external_services']
        init_gRPC(service_mesh_config, globalDict['work_model'], gRPC_port,app)
        # Start the gRPC server
        grpc_thread = gRPCThread()
        grpc_thread.run()
        # Flask HTTP REST server started for Prometheus metrics and for the entry point (s0) that anyway receives REST requests from API gateway
        app.run(host='0.0.0.0', port=8080, threaded=True)
    else:
        app.logger.info("Error: Unsupported request method")
        sys.exit(0)