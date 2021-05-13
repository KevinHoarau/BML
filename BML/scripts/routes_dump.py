import sys
from BML.data.routes import dumpRoutes

def parseArgs(args):
	if(len(args)<4 and len(args)!=3):
		quit("Usage: " + args[0] + " startTime endTime outputFolder [collector_1] ... [collector_n]" + '\n' +
			 "       " + args[0] + " updatesDumpFile outputFolder")

	start = -1
	end = -1
	updatesDumpFile = None
	folder = None
	collectors = []

	if(len(args)>3):
		start = int(args[1])
		end = int(args[2])
		folder = args[3]

		collectors = []
		if(len(args)>4):
			for i in range(4, len(args)):
				collectors.append(args[i])

	elif(len(args)==3):
		updatesDumpFile = args[1]
		folder = args[2]

	return((start, end, updatesDumpFile ,folder, collectors))

def main():

	(start, end, updatesDumpFile ,folder, collectors) = parseArgs(sys.argv)

	dumpRoutes(start, end ,folder, collectors, updatesDumpFile=updatesDumpFile)
	

if __name__== "__main__":
	main()