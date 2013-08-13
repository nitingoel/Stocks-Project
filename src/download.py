import multiprocessing
from extractors import ExtractorFactory
from extractors import ParallelExtractor
from time import time


input_file = open('symbols.txt','r')
error_file = open('errors.txt','w')

symbols = []
oFiles = []

t0 = time()
for symbol in input_file:
	symbol= symbol.rstrip('\r\n')
	ofile = symbol + '.csv'
	symbols.append(symbol)
	oFiles.append(ofile)

	"""
	print("Downloading data for "+symbol)
	try:
		extractor = ExtractorFactory().getExtractor("yahoo")
		extractor.setEndDate(31, 12, 2013)
		extractor.setEndDate(1, 1, 2005)
		extractor.getSymbolDataToFile(symbol, ofile)
	except Exception as e:
		print "Error downloading data for "+symbol
		error_file.write(symbol+': Unable to download data\n')
		error_file.write("Error description is: "+str(e))
		error_file.flush()
		continue
	"""

extractor = ExtractorFactory().getExtractor("yahoo")
extractor.setEndDate(31, 12, 2013)
extractor.setStartDate(1, 1, 2005)


if __name__ == '__main__':    
	multiprocessing.freeze_support()
	pExtractor = ParallelExtractor(extractor)
	pExtractor.getSymbolDataToFileInParallel(symbols, oFiles, errorFile='errors.txt', numProcesses=4, numThreads=32)
	t1 = time()
	print 'Time taken for execution is %f' %(t1-t0)

print "All Done"