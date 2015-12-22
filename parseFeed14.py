#!/usr/bin/env python

# The contents of this file are copyrighted.
# (c) 2012, Regents of the University of California. All rights reserved.

# Get ready for Python 3.x
from __future__ import unicode_literals, print_function, absolute_import, division

# System modules
import cookielib, codecs, filecmp, hashlib, os, re, shutil, sys, time, \
       urllib, urllib2, urlparse, xml
import subprocess
from datetime import datetime
from xml.dom import minidom
from os.path import join as pjoin
from xml.etree import ElementTree as etree
from decimal import *
class MyHTTPRedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_302(self, req, fp, code, msg, headers):
        return urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)

###############################################################################
# Global variables

global urlOpener, filesToFetch, collection

###############################################################################
def processFeed(feedURL, feedText):
  """ Parse what we need from an ATOM feed page, and return the URL of the next
      thing to fetch, or None if we're done. """

  global filesToFetch
  ns = {
	'atom': 'http://www.w3.org/2005/Atom',
	'dct': 'http://purl.org/dc/terms/'
  }
  feedData = etree.fromstring(str(feedText))

  for entry in feedData.findall("./atom:entry", ns):
    idElement = entry.findall("./atom:id", ns)
    id = idElement[0].text if len(idElement) else None
    id = re.sub("^http(s?)://[^/]+/", "", id) # Just retain the id part
    objSize = entry.findall("./dct:extent", ns)[0].text
    lastMod = entry.findall("./atom:updated",ns)[0].text
#    lastMod="2015-10-05"
    for link in entry.findall("./atom:link[@rel='http://purl.org/dc/terms/hasPart']", ns):
      href = link.get('href', '')
      m = re.search("\/d\/(.*?)\/(.*?)\/(.*)", href)
      version = m.group(2) if m else ' '
      file = urllib2.unquote(m.group(3)) if m else ''
	   
      if (file != 'producer/mrt-datacite.xml') and (file!='producer%2fmrt-datacite.xml'):
        continue
      
      if (objSize):
		objectSize = getReadableObjSize(objSize)
      else:
		objectSize="1"
      
      filesToFetch.append((id, file, objectSize, version, lastMod, urlparse.urljoin(feedURL, href)))

  nextFeedLink = feedData.findall("./atom:link[@rel='next']", ns)
  return urlparse.urljoin(feedURL, nextFeedLink[0].get('href')) if len(nextFeedLink) else None

#############################################################################
def getReadableObjSize(objSizeStr):
#this proc transforms the raw byte total into dd.d B/Mb/Gb/Tb string
#the bytesize reported in the Atomfeed is the total size of all versions--we'll want to figure
#and return only the size of the most recent version
  objSizeInt=int(objSizeStr)
  interimObjSizeDec=1.0
  objSize=""
  if objSizeInt < 1000:
	objSize = str(objSizeInt) + " B"
  elif objSizeInt < 1000000:
	interimObjSizeDec=objSizeInt/1000
	objSize = str(round(interimObjSizeDec,1)) + " Kb"
  elif objSizeInt < 1000000000:
	interimObjSizeDec=objSizeInt/1000000
	objSize = str(round(interimObjSizeDec,1)) + " Mb"
  elif objSizeInt < 1000000000000:
	interimObjSizeDec=objSizeInt/1000000000
	objSize = str(round(interimObjSizeDec,1)) + " Gb"
  elif objSizeInt < 1000000000000000:
	interimObjSizeDec=objSizeInt/1000000000000
	objSize = str(round(interimObjSizeDec,1)) + " Tb"
  else:
	objSize = str(objSizeInt)
  return(objSize)
  
#############################################################################
def idToPath(baseDir, id):
  """ Translate an identifier to a PairTree path. """

  # Escape certain visible ASCII, and all non-visible ASCII
  transId = re.sub('["*+,<=>?^]|[^\x21-\x7e]', \
                   lambda m: "^%02x" % ord(id[m.start()]), \
                   id)

  # The remainder is now a plain string.
  transId = str(transId)
  
  # There are three single-character translations to make as well
  transId = transId.replace("/", "=").replace(":", "+").replace(".", ",")

  # Pair up the characters to form a pairpath
  outPath = pjoin(baseDir, campusName)
  outPath = pjoin(outPath, transId)

# Done.
  return outPath
  
#############################################################################
def checkLogin(url):
  """ Attempt to fetch a file, and if we get a Merritt login page instead,
      try to log in. """
  global urlOpener
#  this line is used to bypass DUAs
  if campusName=="ucsf":
	url+="?blue=true"

# build opener
  opener = urllib2.build_opener( urllib2.HTTPCookieProcessor() )
  urllib2.install_opener( opener )
# get file  
  try:
 	_file = opener.open(url)
  except Exception, e:
#  except IOError, e:
    print ("%s: We failed to open %s" % (str(datetime.now()),url))
    if hasattr(e, 'code'):
        print ("%s: We failed with error code - %s." % (str(datetime.now()),e.code))
    if hasattr(e, 'reason'):
        print ("%s: The error object has the following reason :" % (str(datetime.now()),e.reason))
	sys.exit(1)
  else:
	return _file



###############################################################################
if __name__ == '__main__':
  """ The main program begins here """

  global urlOpener, filesToFetch, idsafe, token
  
  # Check that we got a URL on the command line
  if len(sys.argv) != 3 or "http" not in sys.argv[1]:
    sys.stderr.write("Usage: " + __file__ + " merrittFeedURL campus(abbr)\n")
    sys.exit(1)

  # We need cookie handling, so make an opener to do that
  cookieJar = cookielib.CookieJar()
  cookieprocessor = urllib2.HTTPCookieProcessor()
  urlOpener = urllib2.build_opener(MyHTTPRedirectHandler, cookieprocessor)
  urllib2.install_opener(urlOpener)
  # Let Merritt know who we are
  urlOpener.addheaders = [('User-agent', 'CDL accro2 fetcher')]

  # Find the collection identifier
  url = sys.argv[1]
  campusName = sys.argv[2]
  
  m = re.search("(http(s?)://[^/]+).*collection=([^&;]+)", url)
  assert m, "Cannot find collection identifier in feed URL"
  server = m.group(1)
#  collection = m.group(3)

# Scan through the collection's feed
  filesToFetch = []
  while url:

    # Grab the feed
    print("Feed:  %s" % url)
    req = urllib2.Request(url)

    try:
       stream = urlOpener.open(req)
    except Exception, e:
        print ("%s: Open error: %s" % (str(datetime.now()), e))
        sys.exit(1)

    try:
     # And process it.
      url = processFeed(url, stream.read())
    except Exception, e:
        print ("%s: Read error: %s" % (str(datetime.now()), e))
        sys.exit(1)
    finally:
      stream.close()

  if len(filesToFetch) > 0:
    # Now fetch all the files
    
    for (id, fileName, objectSize, versionStr, lastMod, url) in filesToFetch:
      dstPath = idToPath("data", id)
      p = re.search("producer/(.*)", fileName)
      newFileName= p.group(1) if p else fileName
      idsafe = re.sub(r':','%3a',id)
      idsafe = re.sub(r'/','%2f',idsafe)
# The way to determine the DOI may change depending on EZID "alias" (formerly known as 
# a "shadow ARK"
      if "ark:/b" in id:
		doi = re.sub(r'ark:/b','doi:10.',id)
      elif "ark:/c" in id:
		doi = re.sub(r'ark:/c','doi:10.1',id)
      else:
		doi = id
	  
      if not os.path.exists(dstPath):
        os.makedirs(dstPath)

      try:
        stream=checkLogin(url)
      except:
		sys.exit(1)
		
      try:
        target = pjoin(dstPath, newFileName)
        xmldoc=minidom.parse(stream)
      
      except Exception, e:
        print ("%s: %s is NOT well-formed! %s" % (str(datetime.now()),dstPath, e))
	
      else:
        target = pjoin(dstPath, newFileName)
        print("target: %s" % target)
        with open(target, "w") as f:
		  f.write(xmldoc.toxml("utf-8"))
#   		  shutil.copyfileobj(stream, f)

        # Also write a file recording the target link. This is temporary until
        # the ATOM feed includes the link for us.
        target = pjoin(dstPath, "target_link")

		# this will create a link to the most recent version, rather than the entire object	
        with open(target, "w") as f:
          f.write("%s/d/%s/%s\n" % (server, idsafe, versionStr))

        doiLink = pjoin(dstPath, "doi")   
        with open(doiLink, "w") as f:
		  f.write("%s" % doi)

        ObjectSize=pjoin(dstPath, "objectSize")
        with open(ObjectSize, "w") as f:
		  f.write("%s" % objectSize)
		
        LastModified=pjoin(dstPath, "lastMod")
        with open(LastModified, "w") as f:
		  f.write("%s" % lastMod)

      finally:
		stream.close()
		

