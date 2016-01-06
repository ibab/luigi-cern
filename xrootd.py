
import luigi
from luigi.target import FileSystemTarget, FileSystem
from urlparse import urlparse

import XRootD.client as client

class XRootDFileSystem(FileSystem):

    def __init__(self, url):
        self._fs = client.FileSystem(url)

    def exists(self, path):
        resp, info = self._fs.stat(path)
        if resp.errno == 3011:
            # File not found
            return False
        elif resp.code == 0:
            return True
        else:
            raise RuntimeError(str(resp))
    
    def remove(self, path, recursive=True):
        if recursive:
            self._fs.rmdir(path)
        else:
            self._fs.rm(path)

    def mkdir(self, path):
        self._fs.mkdir(path)

    def isdir(self, path):
        if self.exists(path):
            try:
                self.listdir(path)
            except ValueError:
                return False
            return True
        return False

    def listdir(self, path):
        resp, info = self._fs.dirlist(path)
        if resp.errno:
            raise ValueError(str(resp))
        else:
            return map(lambda x: x.name, info)

class XRootDTarget(FileSystemTarget):

    def __init__(self, url, fs=None):
        self._url = url
        result = urlparse(url)
        if result.scheme != 'root':
            raise ValueError('A url of the form root://... is expected')
        self._host = result.netloc
        self.path = result.path

        if fs is None:
            self._fs = XRootDFileSystem('root://{}'.format(self._host))
        else:
            self._fs = fs

    @property
    def fs(self):
        return self._fs
    
    def open(self, mode='r'):
        f = client.File()

        flags = None

        if mode == 'r':
            flags = client.flags.OpenFlags.READ
        elif mode == 'w':
            if self.exists():
                flags = client.flags.OpenFlags.UPDATE
            else:
                flags = client.flags.OpenFlags.NEW
        else:
            raise ValueError("Unsupported open mode '%s'" % mode)

        f.open(self._url, flags)

        return f

