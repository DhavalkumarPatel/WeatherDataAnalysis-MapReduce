step 1: On unzipping my solution you will get following deliverables in one folder:
	1. Report - pdf document as required
	2. Source Code - Java Maven Project source code along with Makefile to run (see step 2 & 3 for running the code)
	3. AWS Syslog Files - 4*2 plain text syslog files (4 program and 2 times each program run)
	4. AWS Output Files - 4 sets of part-r-... (output) files
	5. Sequential Program log and output (just for information)

Step 2: Run ClimateAnalysis Source Code (all programs) in local (location:: 2. Source Code/ClimateAnalysis)

	- Copy the input file(s) into input folder (ClimateAnalysis/input)
	- Open terminal and cd to this ClimateAnalysis directory
	- run following commands to run the different programs
		1.a-NoCombiner          : make job.name="mr.hw2.part1.NoCombiner" alone
		1.b-Combiner            : make job.name="mr.hw2.part1.Combiner" alone
		1.c-InMapperComb        : make job.name="mr.hw2.part1.InMapperComb" alone
		1.d-HW1-Sequential	: make job.name="mr.hw1.Sequential" alone
		2-TemperatureTimeSeries : make job.name="mr.hw2.part2.TemperatureTimeSeries" alone
	- On running any of the program, you can find the output results in output folder (ClimateAnalysis/output)

Step 3: Run ClimateAnalysis Source Code (all programs) on aws (location:: 2. Source Code/ClimateAnalysis)

	- Copy the input file(s) into input folder (ClimateAnalysis/input)
	- Open terminal and cd to this ClimateAnalysis directory
	- run "make upload-input-aws" command to upload the input to aws (it will copy the input files into dspatel28 bucket)
	- run following commands to run the different programs
		1.a-NoCombiner          : make job.name="mr.hw2.part1.NoCombiner" cloud
		1.b-Combiner            : make job.name="mr.hw2.part1.Combiner" cloud
		1.c-InMapperComb        : make job.name="mr.hw2.part1.InMapperComb" cloud
		2-TemperatureTimeSeries : make job.name="mr.hw2.part2.TemperatureTimeSeries" cloud
	- On running any of the program, you can find the output results and logs in dspatel28 bucket
	- After execution, you can delete all data from dspatel28 bucket by running "make delete-s3-aws" command



Notes: 

1. Missing values for temperatures (records with -9999 temperatures) are ignored
2. If any station does not have either of the temperature (TMAX or TMIN) then it is shown by NULL (for all programs)
3. In part 2, for any station, If there is no record for any specific year (Both TMAX and TMIN are not present) then that 
   year is not considered while emitting, so you will not get any records like (year, NULL, NULL) for any station.



