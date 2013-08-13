import multiprocessing
from multiprocessing import Process
from abc import ABCMeta, abstractmethod
from datetime import date
from utilities import DateValidator
import urllib2
import threading
import settings
		
class ParallelThreadRunner(threading.Thread):
	""" This class represents a single instance of a running thread"""
	
	def __init__(self, threadID, procID, name, extractor, symbolArray, outFileArray, errorFile, start, end):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.prodID = procID
		self.name = name
		self.linearExtractor = extractor
		self.symbolArray = symbolArray
		self.outFileArray = outFileArray
		self.start_index = start
		self.end_index = end
		self.errorFile = errorFile
		
	def run(self):
		symbolArray = self.symbolArray
		outFileArray = self.outFileArray
		start = self.start_index
		end = self.end_index
		errorFile = self.errorFile
		
		if (len(symbolArray)-1) < end:
			end = len(symbolArray)-1
		if (start < 0 or end < start or end > (len(outFileArray)-1)) :
			raise Exception("Incorrect bounds send to thread for execution")

		error_file = open(errorFile, "w")
		for x in range(start, end+1):
			#print("Thread ",self.threadID, " named ",self.name, " downloading data for symbol ",symbolArray[x])
			try:
				self.linearExtractor.getSymbolDataToFile(symbolArray[x], outFileArray[x])
				error_file.write(symbolArray[x]+"\tThread: " + self.name + " successfully downloaded data for symbol " + symbolArray[x] + "\n")
			except Exception as e:
				error_file.write(symbolArray[x]+"\tThread: "+self.name+" - Error downloading data for " + symbolArray[x]  + " - " +str(e))
			error_file.flush()


class ParallelProcessRunner:
	""" This class represents a single instance of a running process """
	def __init__(self,extractor):
		self.LinearExtractor = extractor
	
	def launchThreadsForProcess(self, symbolArray, outFileArray, errorFile, procid, numThreads, proc_start, proc_end):
		if (numThreads > settings.MAX_THREADS):
			numThreads = settings.MAX_THREADS
		if (len(symbolArray) < 1 or len(outFileArray) < 1 or len(symbolArray) != len(outFileArray)) :
			raise Exception("Incorrect size of symbol or outputfile arrays")
		if (len(symbolArray)-1) < proc_end:
			proc_end = len(symbolArray)-1
		if (proc_start < 0 or proc_end < proc_start or proc_end > (len(outFileArray)-1)) :
			raise Exception("Incorrect bounds send to thread for execution")
				
		assignedArraySize = proc_end-proc_start+1
		sliceSize = assignedArraySize/numThreads if assignedArraySize%numThreads == 0 else assignedArraySize/(numThreads-1)
		print "For process ",procid," assignedArraySize = ",assignedArraySize," and sliceSize = ",sliceSize,'\n'
		mythreads = []
		for x in range(numThreads):
			threadName = "Proc-"+str(procid)+"-Thread-"+str(x)
			thread_start = proc_start + sliceSize * x
			if (x == numThreads-1):
				print "Last thread will go with a length of : ",assignedArraySize%(numThreads-1)," instead of the sliceSize of ",sliceSize, "\n"
				thread_end = thread_start + (assignedArraySize%(numThreads-1)) - 1
			else:
				thread_end = thread_start + sliceSize -1 
			th = ParallelThreadRunner(x, procid, threadName, self.LinearExtractor, symbolArray, outFileArray, errorFile+"_"+str(x), thread_start, thread_end)
			print "For Process number "+str(procid)+" Launching thread number "+str(x) + " from "+str(thread_start)+" to "+str(thread_end)+'\n'
			mythreads.append(th) 


		for i in mythreads:
			i.start()
		for i in mythreads:
			i.join()

		try:
			final_error = open(errorFile, "w")
			for i in mythreads:
				err = open(errorFile+"_"+str(i.threadID), "r")
				final_error.write(err.read())
		except:
			print "Error writing output to the final error file \n"

class ParallelExtractor:
	def __init__(self,extractor):
		""" 
		This constructor takes a linear-extractor as a parameter and stores
		it for further processing
		"""
		self.LinearExtractor = extractor
	
	def getSymbolDataToFileInParallel(self, symbolArray, outFileArray, errorFile, numProcesses, numThreads):
		"""
		getSymbolDataToFileInParallel() : Using a linear extractor, launches multiple threads to extract data in parallel
		Parameters are:
			symbolArray: An array of symbols to download
			outFileArray: An array of filenames (with path) where symbol data is to be saved
			errorFile:	An error file where any and all runtime errors are captured.
			numThreads: An integer value between 1-32 of the number of threads to launch in parallel
		"""
		
		if (numProcesses > settings.MAX_PROCESSES):
			numThreads = settings.MAX_PROCESSES
		if (numThreads > settings.MAX_THREADS):
			numThreads = settings.MAX_THREADS
		if (len(symbolArray) < 1 or len(outFileArray) < 1 or len(symbolArray) != len(outFileArray)) :
			raise Exception("Incorrect size of symbol or outputfile arrays")
		
		print "Total symbols to download are : "+str(len(symbolArray))+'\n'
		
		sliceSize = len(symbolArray)/numProcesses if len(symbolArray)%numProcesses == 0 else len(symbolArray)/(numProcesses-1)
		myprocs = []
		for x in range(numProcesses):
			procName = "Process"+str(x)
			start = sliceSize * x
			if (x==numProcesses-1 and len(symbolArray)%numProcesses != 0):
				print "Last Process will go with a length of : ",len(symbolArray)%(numProcesses-1)," instead of the sliceSize of ",sliceSize, "\n"
				end = start + len(symbolArray)%(numProcesses-1) - 1
			else:
				end = start + sliceSize -1  
			ppr = ParallelProcessRunner(self.LinearExtractor)
			pr = Process(target= ppr.launchThreadsForProcess, args=(symbolArray, outFileArray, errorFile+"_"+str(x), x, numThreads, start, end)) 
			pr.procID = x
			print "Launching Process number "+str(x) + "from "+str(start)+" to "+str(end)+'\n'
			myprocs.append(pr) 

		for i in myprocs:
			i.start()

		""" Awaiting the threads to complete execution """
		for i in myprocs:
			i.join()
		
		try:
			final_error = open(errorFile, "w")
			for i in myprocs:
				err = open(errorFile+"_"+str(i.procID), "r")
				final_error.write(err.read())
		except:
			print "Error writing output to the final error file \n"

		
class ExtractorFactory:
	def getExtractor(self, oType):
		if (oType.lower() == "yahoo"):
			return YahooExtractor()
		else:
			raise Exception("Extractor not supported");

# Use this ABC to model all new extractors. The expected data format for the extractor implementations is as follows:
# CSV format with the following columns:
# First line contains the column headers
#Symbol,Date,Open,High,Low,Close,Volume,Adj Close
#
		
class BaseExtractor:
		metaclass=ABCMeta
		@abstractmethod
		def setStartDate(self, date, month, year):
			pass
		@abstractmethod
		def setEndDate(self, date, month, year):
			pass
		@abstractmethod
		def getSymbolDataToFile(self, symbol, filename):
			pass
		@abstractmethod
		def getSymbolData(self, symbol):
			pass
#//] BaseExtractor

class YahooExtractor(BaseExtractor):
	startDate = 1
	endDate = 31
	startMonth = 1
	endMonth = 12
	
	def __init__ (self):
		self.endYear = date.today().year
		self.startYear = self.endYear - 10
	#//] __init__()
	
	def setStartDate(self, date, month, year):
		if DateValidator().validateDate(date, month, year):	
			self.startDate = date
			self.startMonth = month
			self.startYear = year
		else:
			return False
	#//] setStartDate()
	

	def setEndDate(self, date, month, year):
		if DateValidator().validateDate(date, month, year):	
			self.endDate = date
			self.endMonth = month
			self.endYear = year
		else:
			return False
	#//] setEndDate()

	
	def getURL(self, symbol):
		return "http://ichart.finance.yahoo.com/table.csv?s=" + symbol + \
		"&a=" + str(self.startMonth-1) + "&b=" + str(self.startDate) + "&c=" + str(self.startYear) + \
		"&d=" + str(self.endMonth-1) + "&e=" + str(self.endDate) + "&f=" + str(self.endYear) + "&g=d&ignore=.csv"
	#//] getURL()
	

	def getSymbolDataToFile(self, symbol, filename):
		url = self.getURL(symbol)
		try:
			output = open(filename,'w')
			connection = urllib2.urlopen(url)
			firsttime=True;
			for line in connection:
				if firsttime:
					line = "#Symbol,"+line
					firsttime = False
				else:
					line = symbol+","+line
				output.write(line)
			output.close
		except Exception as e:
			raise Exception("URL: "+url+" Exception - "+str(e)+'\n')
	#//] getSymbolDataToFile()


	def getSymbolData(self, symbol):
		url = self.getURL(symbol)
		connection = urllib2.urlopen(url)
		firsttime=True;
		data="";
		for line in connection:
			if firsttime:
				line = "#Symbol,"+line
				firsttime = False
			else:
				line = symbol+","+line
			data = data+line
		return data
	#//] getSymbolData()
	
#//] Class YahooExtractor

######################################################
## Usage Hints

#extractor = YahooExtractor()
#extractor.setStartDate(30, 2, 2000)
#extractor.setEndDate(31, 12, 2013)
#extractor.getSymbolDataToFile('msft', 'output.txt')
#######################################################
