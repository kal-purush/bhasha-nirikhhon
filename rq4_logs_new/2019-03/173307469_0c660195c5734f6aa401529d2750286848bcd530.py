from wsgiref import simple_server

from workers.base import setup_worker


# do not use this in production
# this is a dev only method provided for convenience
def main():
    simple_server.make_server('0.0.0.0', 6092,
                              setup_worker(worker_class="workers.tulip.NetworkWorker", port=6092, worker_name="worker-tulip")).serve_forever()  # nosec


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass