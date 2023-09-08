from setuptools import setup, find_packages
from setuptools.command.install import install
import subprocess, os, sys

class CustomInstallCommand(install):
	def run(self):
		print("Running bgpstreamm install script")
		process = subprocess.Popen(["sh bgpstream_install.sh"], shell=True)
		process.wait()
		print("bgpstreamm install done")
		
		process = subprocess.Popen(["pip install Cython cmake"], shell=True)
		process.wait()
		
		process = subprocess.Popen(["pip install -r requirements.txt"], shell=True)
		process.wait()

		install.run(self)

setup(
	name='BML',
	version='1.0',
	author='Kevin Hoarau',
	author_email='kevin.hoarau@univ-reunion.fr',
	packages=find_packages(),
	description='BML: An Efficient and Versatile Tool for BGP Dataset Collection',
    cmdclass={
        'install': CustomInstallCommand,
    })