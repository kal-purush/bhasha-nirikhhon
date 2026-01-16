from contextlib import contextmanager


@contextmanager
def file_ctxt_mgr(filename, mode):
    try:
        # everything before yield is in __enter__ method
        f = open(filename, mode)
        yield f
        # everything after yield is in __exit__ method
    finally:
        f.close()


with file_ctxt_mgr("file_open_context_manager_using_contextlib.py", "a") as f:
    f.write("\n# a new comment added\n")