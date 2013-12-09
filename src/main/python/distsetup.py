from distutils.core import setup

def run():
    setup(name='ids',
      version='${project.version}',
      url='http://icatproject.org',
      maintainer='The ICAT project',
      maintainer_email='icatproject-support@googlegroups.com',
      py_modules=['ids'],
      )
