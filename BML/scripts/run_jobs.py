"""
Script that execute jobs in parallel from a jobs file.

Args:
	-f (str): path to the jobs file
	-p (int): number of processes to execute in parallel 
"""

import os, json, time
from BML import utils

def main():
	try:

		filepath = utils.getArg("-f")
		nbProcess = utils.getIntArg("-p")
		folder = utils.mkdirPath(os.path.dirname(filepath))

	except utils.MissingArg as e:
		utils.printHelp(" -f jobsFile -p nbProcess")
		quit()

	with open(filepath) as file:

		jobs = json.load(file)

		utils.runJobs(jobs, folder, nbProcess=nbProcess)
		

if __name__ == '__main__':
	main()