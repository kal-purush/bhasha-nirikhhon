import concurrent.futures
import time

done = False

def blah():
  time.sleep(5)
  return "done with blah"

def foo(bar):
  time.sleep(10)
  return "Returned value: {}".format(bar)

def cb():
  done = True

executor = concurrent.futures.ThreadPoolExecutor()
# thread = executor.submit(foo, args=('done!'))
thread = executor.submit(blah)
# thread.add_done_callback(cb)

while thread.running():
  print(done)
  time.sleep(1)

print("Thread is no longer running, global is now {}".format(done))
