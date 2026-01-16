import re
import subprocess
import time

resolve_command = "physical-web resolve "


# match pattern "field": "value"
def ExtractData(input_string, field):
  key_value = re.search('"%s": ".*?"' % field, input_string).group(0)
  value = re.findall('".*?"', key_value)[1][1:-1]  # remove ""
  return value


def CheckOutput(expect, actual):
  for key in expect:
    if expect[key] != ExtractData(actual, key):
      print "%s does not match, " % key,
      return False
  return True


# regular test
def Test1():
  test_url = "https://www.google.com/"
  expect_output = {
      "scannedUrl":
          test_url,
      "resolvedUrl":
          "https://www.google.com/",
      "title":
          "Google",
      "description":
          "Search the world's information, including webpages, images, videos "
          "and more. Google has many special features to help you find exactly"
          " what you're looking for.",
  }
  start_time = int(time.time())
  actual_output = subprocess.check_output(
      resolve_command + test_url, shell=True, stderr=subprocess.STDOUT)
  end_time = int(time.time())
  latency = end_time - start_time

  if (CheckOutput(expect_output, actual_output)):
    print("Test1 passed, latency is %ds" % latency)
    return True
  else:
    print("Test1 failed, latency is %ds" % latency)
    return False


# regular test
def Test2():
  test_url = "https://www.apple.com/"
  expect_output = {
      "scannedUrl":
          test_url,
      "resolvedUrl":
          "https://www.apple.com/",
      "title":
          "Apple",
      "description":
          "See the MacBook Pro, iPhone 7, and AirPods. Explore iPad, Apple "
          "Watch, iOS, watchOS, macOS, and more. Visit the site to learn, buy,"
          " and get support.",
  }
  start_time = int(time.time())
  actual_output = subprocess.check_output(
      resolve_command + test_url, shell=True, stderr=subprocess.STDOUT)
  end_time = int(time.time())
  latency = end_time - start_time

  if (CheckOutput(expect_output, actual_output)):
    print("Test2 passed, latency is %ds" % latency)
    return True
  else:
    print("Test2 failed, latency is %ds" % latency)
    return False


# redirection https://goo.gl/rJw231 => https://www.youtube.com/
def Test3():
  test_url = "https://goo.gl/rJw231"
  expect_output = {
      "scannedUrl":
          test_url,
      "resolvedUrl":
          "https://www.youtube.com/",
      "title":
          "YouTube",
      "description":
          "Enjoy the videos and music you love, upload original content, and "
          "share it all with friends, family, and the world on YouTube.",
  }
  start_time = int(time.time())
  actual_output = subprocess.check_output(
      resolve_command + test_url, shell=True, stderr=subprocess.STDOUT)
  end_time = int(time.time())
  latency = end_time - start_time

  if (CheckOutput(expect_output, actual_output)):
    print("Test3 passed, latency is %ds" % latency)
    return True
  else:
    print("Test3 failed, latency is %ds" % latency)
    return False


# multiple redirection
# http://ow.ly/doMB306qmTW => http://bit.ly/2flWw6L =>
# https://goo.gl/64fiQn => https://www.youtube.com/watch?v=1yaLPRgtlR0
def Test4():
  test_url = "http://ow.ly/doMB306qmTW"
  expect_output = {
      "scannedUrl":
          test_url,
      "resolvedUrl":
          "https://www.youtube.com/watch?v=1yaLPRgtlR0",
      "title":
          "Introduction to the Physical Web (100 days of Google Dev) - YouTube",
      "description":
          "Scott Jenson introduces the Physical Web. The Physical Web is an "
          "extension of the web into the physical world so you can walk up and"
          " interact with any device...",
  }
  start_time = int(time.time())
  actual_output = subprocess.check_output(
      resolve_command + test_url, shell=True, stderr=subprocess.STDOUT)
  end_time = int(time.time())
  latency = end_time - start_time

  if (CheckOutput(expect_output, actual_output)):
    print("Test4 passed, latency is %ds" % latency)
    return True
  else:
    print("Test4 failed, latency is %ds" % latency)
    return False


# redirection to non-https
# http://bit.ly/2fmcYDO => http://ow.ly/U1ye306qxVk => http://www.bbc.com/
def Test5():
  test_url = "http://bit.ly/2fmcYDO"
  expect_output = {
      "scannedUrl": test_url,
      "unresolvedUrl": "http://www.bbc.com/",
      "description": "Result Filtered (Non HTTPS URL).",
  }
  start_time = int(time.time())
  actual_output = subprocess.check_output(
      resolve_command + test_url, shell=True, stderr=subprocess.STDOUT)
  end_time = int(time.time())
  latency = end_time - start_time

  if (CheckOutput(expect_output, actual_output)):
    print("Test5 passed, latency is %ds" % latency)
    return True
  else:
    print("Test5 failed, latency is %ds" % latency)
    return False


if __name__ == "__main__":
  Test1()
  Test2()
  Test3()
  Test4()
  Test5()