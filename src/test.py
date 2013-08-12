import threading
        
class ParallelThreadRunner(threading.Thread):
    def __init__(self, threadID, name, extractor, symbolArray, outFileArray, errorFile, start, end):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.linearExtractor = extractor
        self.symbolArray = symbolArray
        self.outFileArray = outFileArray
        self.start_index = start
        self.end_index = end
        self.errorFile = errorFile
        
    def run(self):
        print "Thread "+self.name+ " started running"
        symbolArray = self.symbolArray
        outFileArray = self.outFileArray
        start = self.start_index
        end = self.end_index
        errorFile = self.errorFile
        
        if (len(symbolArray)-1) < end:
            end = len(symbolArray)-1
        if (start < 0 or end < start or end > (len(outFileArray)-1)) :
            raise Exception("Incorrect bounds send to thread for execution")

        for x in range(start, end):
            error_file = open(errorFile+"_"+str(self.threadID), "w")
            print("Thread ",self.threadID, " named ",self.name, " downloading data for symbol ",symbolArray[x])
            try:
                self.linearExtractor.getSymbolDataToFile(symbolArray[x], outFileArray[x])
            except Exception as e:
                error_file.write("Thread: "+self.name+" - Error downloading data - "+str(e))
                exit
        pass

class ParallelExtractor:
    def __init__(self,extractor):
        self.LinearExtractor = extractor
    
    def getSymbolDataToFileInParallel(self, symbolArray, outFileArray, errorFile, numThreads):
        mythreads = []
        for x in range(0, 4):
            threadName = "Thread"+str(x)
            th = ParallelThreadRunner(x, threadName, self.LinearExtractor, symbolArray, outFileArray, errorFile, 0, 4)
            print "attempting to launch a thread"
            mythreads.append(th) 

        for i in mythreads:
            i.start()

        """ Awaiting the threads to complete execution """
        for i in mythreads:
            i.join()
            
            
class ExtractorFactory:
    pass
# Use this ABC to model all new extractors. The expected data format for the extractor implementations is as follows:
# CSV format with the following columns:
# First line contains the column headers
#Symbol,Date,Open,High,Low,Close,Volume,Adj Close
#
        
class BaseExtractor:
    pass
#//] BaseExtractor

class YahooExtractor(BaseExtractor):
    pass

extractor = YahooExtractor()
pExtractor = ParallelExtractor(extractor)
symbols = ['ABC', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF', 'DEF']
oFiles = ['abc.csv', 'def.csv', 'def.csv', 'def.csv', 'def.csv', 'def.csv', 'def.csv', 'def.csv', 'def.csv', 'def.csv', 'def.csv']
pExtractor.getSymbolDataToFileInParallel(symbols, oFiles, 'errors.txt', 5)
