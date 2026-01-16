#!/usr/bin/python
#-*- coding: utf-8 -*-

import ConfigParser
import os

def getConfig():
  #config
  config_file = "/etc/ganeti/extstorage/3par.conf"
  config = ConfigParser.SafeConfigParser()
  if not config.read(config_file):
    raise ConfigParser.Error("Unable to read config file")

  host        = config.get('3par','host')
  login       = config.get('3par','login')
  password    = config.get('3par','password')
  default_cpg = config.get('3par','default_cpg')
  snap_cpg    = config.get('3par','snap_cpg')

  return (host,login,password,default_cpg,snap_cpg)

def readEnv():
  (host,login,password,default_cpg,snap_cpg) = getConfig() 

  """Read the mandatory enviromental variables
  """
  vol_name           = os.getenv("VOL_NAME")
  vol_cname          = os.getenv("VOL_CNAME")

  if os.getenv("VOL_SIZE") is not None :
    vol_size           = int(os.getenv("VOL_SIZE"))
  else :
    vol_size     = None
  
  if os.getenv("VOL_NEW_SIZE") is not None :
    vol_new_size     = int(os.getenv("VOL_NEW_SIZE"))
  else :
    vol_new_size     = None

  vol_uuid           = os.getenv("VOL_UUID")
  vol_metadata       = os.getenv("VOL_METADATA")
  vol_snapshot_name  = os.getenv("VOL_SNAPSHOT_NAME")

  if os.getenv("VOL_SNAPSHOT_SIZE") is not None : 
    vol_snapshot_size  = int(os.getenv("VOL_SNAPSHOT_SIZE"))
  else : 
    vol_snapshot_size  = None

  vol_open_exclusive = os.getenv("VOL_OPEN_EXCLUSIVE")

  cpg = os.getenv("EXTP_CPG")
  if cpg is None:
    cpg=default_cpg

  return (vol_name,vol_cname,vol_size,vol_new_size,vol_uuid,vol_metadata,vol_snapshot_name,vol_snapshot_size,vol_open_exclusive,cpg)

