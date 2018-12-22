# ModbusDataLogger

This app can be configured to read multiple parameters from Modbus RTU over TCP on a regular basis and store the raw binary result in an SQLite3 database.

It is multithreaded allocating a thread for each gateway allowing simultaneous logging from multiple gateway devices. Multiple slaves and registers can be recorded and will simply be queued in the order they were requested.

## Prerequisites
This script was designed and proven to run on Python3.6 and later. It may run on earlier versions but this cannot be gauranteed. Only one external package is required which is modbus_tk. This can be obtained with: `pip intall modbus_tk` (or `pip3`)


## The SQLite3 Database
Upon first run, the script will create an empty database for you and exit. You must use an external sqlite3 editor such as `sqlite3` to edit the "RequestedData" table and insert/update the parameters you wish to have logged.

### RequestedData table
You must provide:
-  **paramID** - a unique but otherwise arbitrary integer which is the primary key for this table
-  **required** - Either 0 or 1 to denote whether you require this parameter to be logger or not. Setting this to 0 will "pause" logging.
-  **host** - Text giving the fully qualified domain name or IP address of the Modbus Gateway
-  **port** - Integer giving the TCP port of the gateway - typically this is 502
-  **period** - How often in integer seconds (>0) to retreive this parameter. e.g. 10 will retreive this every 10 seconds. If the logger can't run fast enough due to traffice, contention, or too many items in the queue for that gateway, it will simply miss readings
-  **address** - Integer slave address of the modbus RTU device behind the gateway
-  **function** - Integer function number. Non standard functions are valid.
-  **register** - Integer register number starting from 0 in raw form.
-  **count** - Integer number of words (16 bits) to retreive
-  **comment** - Optional free text for your own notes.

When the script runs, it will regularly check this table for changes and so there is no need to restart the script if you wish to make changes to the RequestedData. The logger will attempt to obtain the parameters and save results in the ResultsRaw table with fields as below.

### ResultsRaw table
-  **paramID** - The ID number from the above.
-  **timestamp** - the UNIX timestamp when the data was obtained i.e. integer number of seconds since 1st Jan 1970
-  **rawresult** - Blob/Binary object with the raw binary result.

Note that if a record is deleted from the RequestedData table, all results will also be deleted. To stop logging but keep results, simply update the required field to 0.

## The config.cfg Configuration File
The config.cfg file can have the following settings:

### [DEFAULT]
This section will be created if it doesn't exist with the following defaults
- dbpath = Logging.db *This is the path to the SQLite3 file.*
- logginglevel = WARNING *The level of debug data that will be outputted to stdout*
- watchdogperiod = 1 *How often in seconds that the data logging threads will check for changes*
- datathreadperiod = 60 *How often in second that accumalated data will get saved. Larger numbers will reduce data thrashing but risk more data being lost if the system crashes.*

### [RUNTIME]
This section is added and updated by the app and should not be changed by users as its for information only
- starttime = 2018-12-22T10:39:25.668189+00:00 *This will be the time the script began*
- pid = 13088 *This will be the pid of the process which can be used to terminate*

## Using within a wider system
This logger is intended to be run continuously and can be safely terminated by Ctrl+C, or sending it a TERM signal. In both cases, it will save all data and safely terminate but there may be a lag while it does so. Any other attempt to forcelly kill the process may result in lost data or corruption. In long term usage, you may wish to run this as a background or daemoned process with stdout/sterr being piped into a log file.

An external seperate app can submit tasks to the RequestedData table and retreive results and this can be done while the logger is running. It would be up to this external app to interpret/decode the results. Other tables can be created as the user wishes within the IO contrainst of SQLite3.

## Maintenance & Support
This project is solely maintained and supported by Mark Jarvis on a best effort basis. For help or to identify bugs, feel free to contact me or raise issues.
