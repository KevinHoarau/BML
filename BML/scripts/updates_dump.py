import sys
from BML.data.updates import dumpUpdates

def parseArgs(args):
	if(len(args)<4):
		quit("Usage: " + args[0] + " startTime endTime outputFolder [collector_1] ... [collector_n]")

	start = int(args[1])
	end = int(args[2])
	folder = args[3]

	collectors = []
	if(len(args)>4):
		for i in range(4, len(args)):
			collectors.append(args[i])
	return((start, end, folder, collectors))

def main():

	(start, end, folder, collectors) = parseArgs(sys.argv)

	dumpUpdates(start, end, folder, collectors)
	
if __name__ == '__main__':
	main()
