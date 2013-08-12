import time

class DateValidator:
	def oldValidateDate(self, day, month, year):
		print "Day: ", day, " Month: ",month," Year: ",year,'\n'
		if (month < 1 or month > 12):
			return False
		if (day < 1 ):
			return False
		if (month in [1,3,5,7,8,10,12]):
			if (day > 31):
				return False
		if (month in [4,6,9,11]):
			if (day > 30):
				return False
		if month == 2:
			if ((year%400 == 0)  or (year%100 !=0 and year%4 == 0)):
				if (day > 29):
					return False
			else:
				if (day > 28):
					return False
		return True
	#//] oldValidateDate()
	
	
	def validateDate(self, date, month, year):
		key = str(date) + " " + str(month) +" "+ str(year)
		try:
			time.strptime(key, "%d %m %Y")  
			return True
		except:
			return False
	#//] validateDate()

#// End Class DateValidator
