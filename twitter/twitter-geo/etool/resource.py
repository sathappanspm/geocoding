#!/usr/bin/env python

import sys

sys.path.append('/usr/local/lib/python2.7/dist-packages')

# if the java interface PyResource is imported, we're using jython
# and Resource should inherit from that interface
try:
    #import edu.embers
    from edu.embers.etool import PyResource
    USING_JYTHON = True

except:
    USING_JYTHON = False


import __builtin__
import re
import logging
import hashlib
import os
import os.path
import codecs
import time
import conf
import logs
from boto import connect_s3
import errno
import argparse
import posixfile #jython doesnt support fcntl
import tarfile

log = logging.getLogger(__name__)

PROD_RES_BUCKET = "pythia-data"

def init(args=None):
	cf = None
	conf.init(args)



def get_latest_key(args,resbucket,prefix):
	'''
	Given bucket, prefix and various standard args, look up latest key for the prefix in the bucket
	TODO: the same lookup is re-implemented in several places. Need just one!
	'''

	try:
		c = connect_s3(args.aws_key, args.aws_secret)
		b = c.get_bucket(resbucket)
	except Exception, e:
		log.error('Error connecting to S3/getting bucket %s: %s' % (resbucket, str(e)))
		return None

	latestFile=None
	latestDate = ""
	try:
		l = b.list(prefix=prefix)
		for k in l:
			# Check if the current file is newer than the
			# previously newest file
			if (latestDate<k.last_modified):
				latestFile = k
				latestDate = k.last_modified
	except Exception, e:
		log.error('Error iterating over S3 files in bucket %s with prefix %s: %s' % (resbucket, prefix, str(e)))
		return None

	return b.get_key(latestFile.name)

def release_lock(f):
        '''
        Release lock on file f
        '''
        if not f:
                return
        f.lock('u')
        #flock(f, flock.LOCK_UN)
        f.close()

def wait_for_unlock(lock_fpath):
        '''
        If the file is locked when this process tries to open it, just
        wait for it to unlock before proceeding
        '''
        f = posixfile.open(lock_fpath, 'w')
        #f = open(lock_fpath, 'w')
        got_lock = False
        while not got_lock:
                # try locking it, if lock achieved, file processing is complete
                try:
                        f.lock('w|')
                        #flock(f, flock.LOCK_EX | flock.LOCK_NB)
                        got_lock = True
                # otherwise, continue to try
                except:
                        got_lock = False
        release_lock(f)

def get_lock(lock_fpath):
        '''
        Check if the lock file is locked by another process
        If not, lock it and return the file object
        If yes, return None and wait for the file to unlock
        '''
        f = posixfile.open(lock_fpath, 'w')
        #f = open(lock_fpath, 'w')
        try:
                f.lock('w|')
                #flock(f, flock.LOCK_EX | flock.LOCK_NB)
                return f
        except IOError:
                return None
        except:
                log.error("Error locking file: %s" % lock_fpath)
                return None


def get_contents_into_local_file(args,key,tmpdir, locking=False, unzipfile=False):
	'''
	Given key, local dirpath and various standard args, make a local copy of the file
	and return the local filepath of the copy.
	If locking is required, lock before download/processing, release lock before returning
	TODO: the same is re-implemented in several places. Need just one!
	'''

	fpath = None
	key_name = key.name

	if tmpdir:
		dn = tmpdir + "/" + os.path.dirname(key_name)
	else:
		dn = os.path.dirname(key_name)

	fn = os.path.basename(key_name)

	fpath = dn + "/" + fn

	# if locking required, check to see if a lock is in place, if yes, wait for unlock and exit
	if locking:
		lock_fpath = fpath + '.exclusivelock'
		try_lock = get_lock(lock_fpath)
		if not try_lock:
			wait_for_unlock(lock_fpath)
			if unzipfile:
				fpath = dn +'/'+ fn[:-len(".tar.gz")]
			return fpath

	if os.path.exists(fpath):
		log.warning("Local file %s already exists, overwriting" % fpath)

	else:
		try:
			os.makedirs(dn)
			log.info("Created local dir %s" % dn)
		except OSError, eOS:
			if eOS.errno != errno.EEXIST:
				log.error("Error when creating local dir %s: ERRNO=%d, %s" % (dn, eOS.errno, str(eOS)))
				if locking:
					release_lock(try_lock)
				return None
	try:
		key.get_contents_to_filename(fpath)
	except Exception, e:
		log.error('Error getting contents into local file %s: %s' % (fpath,str(e)))
		if locking:
			release_lock(try_lock)
		return None

	if unzipfile:
		with tarfile.open(fpath) as tf:
			# for now, assume contents of zip are named the same
			tf.extract(path=dn)
			fpath = dn +'/'+ fn[:-len(".tar.gz")]
			if not os.path.isfile(fpath):
				log.error("Cannot find expected output of unzip: %s" % fpath)

	if locking:
		release_lock(try_lock)
	return fpath

def resource_from_string(name, aws_key='', aws_secret='', resbucket='',
						embers_conf='', tmpdir='', nolocalfile='',
						locking=False, unzipfile=False):
    '''
	Receive necessary arguments as strings from java and return args-like object
	    aws_key = key for aws S3 access
	    aws_secret = secret id for aws S3 access
	    resbucket = bucket where resources are located
	    embers_conf = embers conf file
    tmdir = directory to pull resource to
    nolocalfile = ?
    '''

    # conform to standard Resource construct that takes parser args
    parser = argparse.ArgumentParser()
    parser.add_argument('--aws_key', default = aws_key)
    parser.add_argument('--aws_secret', default = aws_secret)
    parser.add_argument('--aws_resbucket', default = resbucket)
    parser.add_argument('--embers_conf', default = embers_conf)
    args = parser.parse_args([])
    #print args

    r = Resource(name, args, tmpdir, nolocalfile, locking, unzipfile)
    #print r
    return r


class Resource(PyResource if USING_JYTHON else object):

	def __init__(self, name, args, tmpdir, nolocalfile=False, locking=False, unzipfile=False, **kw):
		'''
			name = name of the resource entry in embers.conf (sort of like a queue enrty)
			args = std args
			tmpdir = local dir to dump copy of resource to
		'''


		self._name = name


		res_cfg = conf.get_resource_info(name)
                # needed to rename _localfpath and _key because of jython's introduction of get/set methods
		self._local_fpath = None
  		self._resbucket = None
  		self._prefix = None
  		self._latestkey = None

		if not res_cfg:
			self._local_fpath = name
			log.info("LOCAL Resource fpath=%s" % self._local_fpath)
		else:

			try:
				self._prefix = res_cfg['prefix']
				self._resbucket = args.resbucket

				self._latestkey = get_latest_key(args,self._resbucket,self._prefix)
				if not self._latestkey:
					raise Exception('Failed to get latest key ')

				if nolocalfile:
					self._local_fpath = None
				else:
					self._local_fpath = get_contents_into_local_file(args,self._latestkey,tmpdir,locking,unzipfile)
					if not self._local_fpath:
						raise Exception('Failed to get latest contents to local file ')

				log.info("S3 Resource %s: b=%s pfx=%s key=[%s %s %s] fpath=%s" %
			 		(self._name,
			  		self._resbucket,
			  		self._prefix,
			  		self._latestkey.name,
			  		self._latestkey.last_modified,
			  		self._latestkey.version_id,
			  		self._local_fpath))
			except Exception, e:
				raise Exception('Error when loading resource %s: %s' % (self._name,str(e)))


	def get_key(self):

		return self._latestkey

	def get_contents_as_string(self):

		if not self._latestkey:
			return None

		return self._latestkey.get_contents_as_string()

	def get_localfpath(self):

		return self._local_fpath

	def get_resource_info(self):

		out = {}
		if self._resbucket and self._latestkey:
			out["path"] = "s3::" + self._resbucket + ":" + self._latestkey.name
			if self._latestkey.version_id and not self._latestkey.version_id.lower() == "null":
				out["versionId"] = self._latestkey.version_id
			out["lastModified"] = self._latestkey.last_modified
		else:
			out["path"] = "localfile::"+self._local_fpath

		return out

	def get_resource_info_single(self, infokey):
		return self.get_resource_info()[infokey]


