# This simple selects only bsodmon plugin, to avoid
# performance hits and to be used to benchmark a binary execution speed,
# in order to compare the performance cost of procmon and filetracer in test_injection.

# we need to activate at least one plugin otherwise Drakvuf will enable all of them,
# so bsodmon, since it never triggers (hopefully !)

# Note: You need to use ANSIBLE to trigger the execution, it's the only injection method
# where we can know when the binary has finished its execution without introspection method

PLUGIN_LIST = ['bsodmon']


def test_raw_perf(ev_queue):
    for event in iter(ev_queue['queue'].get, None):
        assert (event['Plugin'] != 'bsodmon')
    assert(ev_queue['completed'].is_set())