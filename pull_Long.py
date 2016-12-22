# Program to pull NWM output from NCEP FTP, thin output to
# only SWE and Snow Depth.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import datetime
from netCDF4 import Dataset
from ftplib import FTP
import subprocess
import sys
import numpy as np
import smtplib
from email.mime.text import MIMEText

# Establish directories
outDir = "/d4/karsten/NWM/Long"
ftpDir = "pub/data/nccf/com/nwm/prod"
tmpDir = "/home/karsten/tmp"

pid = os.getpid()
lockFile = tmpDir + "/get_NWM_Long.lock"

def errOut(msgContent,emailTitle,emailRec):
   msg = MIMEText(msgContent)
   msg['Subject'] = emailTitle
   msg['From'] = emailRec
   msg['To'] = emailRec
   s = smtplib.SMTP('localhost')
   s.sendmail(emailRec,[emailRec],msg.as_string())
   s.quit()
	# Remove lock file
   lockFile = '/home/karsten/tmp/get_NWM_Long.lock'
   os.remove(lockFile)
   sys.exit(1)

def warningOut(msgContent,emailTitle,emailRec):
   msg = MIMEText(msgContent)
   msg['Subject'] = emailTitle
   msg['From'] = emailRec
   msg['To'] = emailRec
   s = smtplib.SMTP('localhost')
   s.sendmail(emailRec,[emailRec],msg.as_string())
   s.quit()
   sys.exit(1)

emailAddy = 'karsten@ucar.edu'
errTitle = 'Error_pull_NWM_Long'
warningTitle = 'Warning_pull_NWM_Long'

# First check to see if lock file exists, if it does, throw error message as
# another pull program is running. If lock file not found, create one with PID.
if os.path.isfile(lockFile):
	fileLock = open(lockFile,'r')
	pid = fileLock.readline()
	warningMsg =  "WARNING: Another Long Pull Program Running. PID: " + pid
	warningOut(warningMsg,warningTitle,emailAddy)
else:
   fileLock = open(lockFile,'w')
   fileLock.write(str(os.getpid()))
   fileLock.close()

# Establish number of hours to go back and check for data.
numHoursBack = 28
hoursPad = 18

# Loop back and pull each six hour cycle of 720 forecasted hours (24 hour intervals).
# Also pull for each of the four ensemble members. Process data into thinned
# output NetCDF files.
dCurrent = datetime.datetime.now()
for hoursBack in range(numHoursBack,hoursPad,-1):
	dCycle = dCurrent - datetime.timedelta(seconds=3600*hoursBack)

	# Only process 00,06,12, and 18z cycle.
	if dCycle.strftime('%H') == "00" or dCycle.strftime('%H') == "06" or \
		dCycle.strftime('%H') == "12" or dCycle.strftime('%H') == "18":
		# First check to see if directory exists on our side
		outDirFinal = outDir + "/" + dCycle.strftime('%Y%m%d') + "/" + \
						  dCycle.strftime('%H')
		if not os.path.isdir(outDirFinal):
			# Directory needs to be created, and data needs to be processed.
			try:
				os.makedirs(outDirFinal)
			except:
				errMsg = "ERROR: Failure to Create Dirtory: " + outDirFinal
				errOut(errMsg,errTitle,emailAddy)

		# Loop through 720 forecasted hours (24-hour output), download and
		# process data.
		for fHour in range(24,721,24):
			fHourStr = str(fHour)
			fHourStr = fHourStr.zfill(3)

			# Loop through four ensemble members
			for ensMem in range(1,5):
				fileDownload = "nwm.t" + dCycle.strftime('%H') + \
            	            "z.long_range.land_" + str(ensMem) + ".f" + fHourStr + ".conus.nc.gz"
				fileOut = outDirFinal + "/NWM_SNOW_LSM_LONG_M" + str(ensMem) + "_" + dCycle.strftime('%Y%m%d%H') + \
							 "_F" + fHourStr + ".nc"
				fileTmp = "nwm.t" + dCycle.strftime('%H') + "z.long_range.land_" + str(ensMem) + ".f" + \
            	       fHourStr + ".conus.nc"

				# Check if data has already been processed
				if os.path.isfile(fileOut):
					continue
				else:
					# Create FTP instance
					try:
						ftp = FTP('ftp.ncep.noaa.gov')
						ftp.login()
					except:
						errMsg = "ERROR: Unable to FTP to ftp.ncep.noaa.gov"
						errOut(errMsg,errTitle,emailAddy)

					ftpDir = "pub/data/nccf/com/nwm/prod/nwm." + dCycle.strftime('%Y%m%d') + \
								"/long_range_mem" + str(ensMem)
					ftpPath = ftpDir + "/" + fileDownload
					try:
						ftp.cwd(ftpDir)
						fileList = ftp.nlst()
					except:
						errMsg = "ERROR: Unable to Change FTP to Directory: " + ftpDir
						errOut(errMsg,errTitle,emailAddy)

					# Loop through file list and check to make sure data exists on server
					check = 0
					for file in fileList:
						if file == fileDownload:
							check = 1

					if check != 1:
						errMsg = "ERROR: Expected to Find: " + fileDownload + " File not Found on FTP Server."
						errOut(errMsg,errTitle,emailAddy)

					# Check if data has already been processed
					if os.path.isfile(fileOut):
						continue

					# Download gzip file
					try:
						cmd = "RETR " + fileDownload
						ftp.retrbinary(cmd,open(fileDownload,'wb').write)
					except:
						errMsg = "ERROR: Unable to Download: " + fileDownload
						errOut(errMsg,errTitle,emailAddy)

					# Quit FTP
					try:
						ftp.quit()
					except:
						errMsg = "ERROR: Unable to successfully exit FTP."
						errOut(errMsg,errTitle,emailAddy)

					# Unzip file
					cmd = "gunzip " + fileDownload
					try:
						subprocess.call(cmd,shell=True)
					except:
						errMsg = "ERROR: Unable to unzip downloaded file."
						errOut(errMsg,errTitle,emailAddy)

					# Read in data, convert to short integer, and output.
					idIn = Dataset(fileTmp,'r')
					idOut = Dataset(fileOut,'w')

					idOut.TITLE = idIn.TITLE
					idOut.missing_value = -9999
					idOut.model_initialization_time = idIn.model_initialization_time
					idOut.model_output_valid_time = idIn.model_output_valid_time

					timeDim = idOut.createDimension('time',)
					latDim = idOut.createDimension('west_east',4608)
					lonDim = idOut.createDimension('south_north',3840)

					timeVar = idOut.createVariable('time','i4',['time'])
					timeVar.long_name = idIn.variables['time'].long_name
					timeVar.units = idIn.variables['time'].units

					sweVar = idOut.createVariable('SNEQV','i2',['time','south_north','west_east'],fill_value=-9999,zlib=True,complevel=2)
					sweVar.MemoryOrder = idIn.variables['SNEQV'].MemoryOrder
					sweVar.description = idIn.variables['SNEQV'].description
					sweVar.units = idIn.variables['SNEQV'].units
					sweVar.scale_factor = 1.0
					sweVar.add_offset = 0.0

					varTmp = idIn.variables['SNEQV']
					dataTmp = varTmp[:,:,:]
					indNdv = np.where(dataTmp == idIn.missing_value)
					dataTmp[indNdv] = -9999
					sweVar[:,:,:] = dataTmp.astype(int)

					# Close NetCDF files
					idOut.close()
					idIn.close()

					# Remove downloaded file
					cmd = "rm -rf " + fileTmp
					try:
						subprocess.call(cmd,shell=True)
					except:
						errMsg = "ERROR: Failure to remove: " + fileDownload
						errOut(errMsg,errTitle,emailAddy)

# Remove lock file
os.remove(lockFile)
