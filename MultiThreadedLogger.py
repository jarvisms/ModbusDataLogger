import sqlite3, sched, time, struct, threading, queue, logging, modbus_tk.modbus_tcp as modbus_tcp
from datetime import datetime, timezone
from configparser import SafeConfigParser
from os.path import dirname, realpath, join, isfile
from os import getpid
import signal
from modbus_tk.modbus import ModbusError, ModbusFunctionNotSupportedError, DuplicatedKeyError, MissingKeyError, InvalidModbusBlockError, InvalidArgumentError, OverlapModbusBlockError, OutOfModbusBlockError, ModbusInvalidResponseError, ModbusInvalidRequestError

# paramID, host, port, address, function, register, count, period
# paramID, timestamp, result

def utcnow(): return datetime.now(tz=timezone.utc).timestamp()	# Function to return a monotomic integer time but with defined start

def TerminateSignal(signum,frame):
	logging.warning('SIGTERM caught. Initiating Shutdown')
	Shutdown.set()
	with ParamsLock:
		ParamsByLoggers = {}

class ThreadSched():
	from random import uniform
	from time import sleep
	
	def newthread(self):
		self.thread = threading.Thread(target=self.sched.run, name='Logger {}:{}'.format(*self.gateway))
	
	def __init__(self, gateway, watchdogperiod):
		self.gateway = gateway
		self.watchdogperiod = watchdogperiod
		self.cache = {}
		self.sched = sched.scheduler(self.utcnow,self.sleep)
		self.sched.enter(self.uniform(self.watchdogperiod*0.5,self.watchdogperiod*1.5),0,self.WatchDog)	# Randomly schedule the watchdog to start
		self.newthread()
	
	def periods(self):
		return set(e.priority for e in self.sched.queue)
	
	def utcnow(self):
		return datetime.now(tz=timezone.utc).timestamp()
		
	def WatchDog(self):	# A regular event which ensures the threads update or can be killed in a sensible timescale without waiting for the next event which could be a long time off
		global Shutdown, ParamsByLoggers, ParamsLock
		logging.debug('Watchdog Started')
		if Shutdown.is_set():	# If this thread is now redundant or external shutdown flag is set, then the queue should be cleared ad watchdog allowd to expire
			for e in self.sched.queue:
				try:
					self.sched.cancel(e)	# Cancel each event still in the queue
				except ValueError:
					pass	# If somehow it was deleted already, just ignore
			self.cache.clear()	# Clears all cached data
			logging.info(f'Watchdog: Shutdown request was received, Schedule has been emptied and Thread will terminate')
		elif self.sched.empty():	# If the queue is empty then there is no more work to be done the the watchdog should be allowed to expire
			logging.warning(f'Watchdog: Thread has an empty schedule and will terminate')
		else:
			with ParamsLock:
				if self.gateway in ParamsByLoggers:
					events = set([ period for period in ParamsByLoggers[self.gateway] ])
					params = set( (ParamsByLoggers[self.gateway][period][item]['address'], ParamsByLoggers[self.gateway][period][item]['function'], ParamsByLoggers[self.gateway][period][item]['register'], ParamsByLoggers[self.gateway][period][item]['count']) for period in events if period in ParamsByLoggers[self.gateway] for item in ParamsByLoggers[self.gateway][period])
				else:
					events, params = set([]), set([])
			for item in set(self.cache) - params:	# For parameters which are locally cached which are no longer required...
				del self.cache[item]	# Delete the cached values as they are no longer needed
				logging.warning('Watchdog: A redundant cached item has been deleted')
			for e in self.sched.queue:
				if e.priority not in events:
					logging.warning(f'Watchdog: A redundant schedule was found by the Watchdog for paramID {self.gateway[0]}:{self.gateway[1]}/{e.priority}')
					try:
						self.sched.cancel(e)	# Cancel each event still in the queue
					except ValueError:
						pass	# If somehow it was deleted already, just ignore
			logging.debug(f'Watchdog reset')
			self.sched.enter(self.watchdogperiod,0,self.WatchDog)

def DataWatchdog(s,watchdogperiod):
	'''Watchdog task for the DataStorer Thread which runs very often and generally does nothing until the global Shutdown signal is set, at which point it reschedules all remaining events to be immediate'''
	global Shutdown
	if Shutdown.is_set():
		for e in s.queue:	# This list will not include this actual watchdog event, so it will effectively be cancelled as it will not be rescheduled
			s.cancel(e)	# Cancel the remaining tasks and...
			s.enter(0,e.priority,e.action,e.argument,e.kwargs)	# Reschedule immediately
			logging.info('Rescheduling Fetch & Store Tasks to take place immediately')
	else:
		logging.debug('DataStore Thread watchdog reset')
		s.enter(watchdogperiod,0,DataWatchdog,(s,watchdogperiod))	# If we aren't due a shutdown, just plod along.

def FetchAndStore(sqllock, sql, Q, s, datathreadperiod, watchdogperiod):
	'''Downloads Results from the Queue and saves everything every to the database'''
	global Shutdown, ParamsByLoggers, ParamsLock, threads
	if not Shutdown.is_set():
		with ParamsLock:	# Fetch the list of requireed modbus devices to be polled
			ParamsByLoggers = FetchJobs(sqllock, sql, ParamsByLoggers)
			if len(ParamsByLoggers) == 0:
				Shutdown.set()
				datathreadperiod = watchdogperiod
		threads = AssignThreads(ParamsLock, ParamsByLoggers, threads, watchdogperiod)	# Dispatch all of the threads as needed.				
	elif not any(threads[gateway].thread.is_alive() for gateway in threads):	# Fall-through from means if Shutdown is set, AND all Threads have terminated
		logging.warning('Shutdown has been requested and all aquisition threads have terminated')
		Q.put(None)	# Insert a sentinel which will should now come after all other results which will then allow this thread to terminate
	else:	# Otherwise Shutdown is requested but threads are still running
		with ParamsLock:
			ParamsByLoggers = {}	# This should kill all treads even if the WatchDogs have stopped.
		datathreadperiod = watchdogperiod
	CanRun = True
	count = 0
	results = None	# Clear out the buffer
	while True:	# Inner loop - pulls all available items from the queue
		try:
			results = Q.get_nowait()	# If there is plenty of stuff in the Queue, this will grab it all
			if results is None:	# This must have arrived via the Queue
				CanRun = False	# If the sentinel is found, break out and termiante
				Q.task_done()
				logging.debug('Sentinel seen in the Queue')
				break
			else:
				with sqllock, sql:	# locks and commit/rollback as needed
					cur = sql.cursor()	# A Single cursor to use throughout the loop, rather than Cursors being created and destroyed by using the connection
					for result in results:
						try:
							cur.execute('INSERT INTO ResultsRaw (paramID, timestamp, rawresult) VALUES (:id,:timestamp,:result);', result)
							count += cur.rowcount
						except sqlite3.DatabaseError as e:
							logging.error(f'DatabaseError with item {result["id"]} @ {result["timestamp"]}, this result is now discarded: {e}')
				Q.task_done()
		except queue.Empty:	# When the queue has been emptied this will break the inner loop
			break
	if count > 0:
		logging.info('Storing {} results'.format(count))
	else:
		logging.debug('No Results found in this pass')
	if CanRun == True:
		logging.debug('Rescheduling Fetch and Store routine')
		s.enter(datathreadperiod,0,FetchAndStore,(sqllock, sql, Q, s, datathreadperiod, watchdogperiod))	# Reschedule
	else:
		logging.info('Fetch and Store routine is not being rescheduled and will soon terminate')

def ModbusRaw(cache, Q, s, gateway, period, now):
	'''Given Modbus details, fetch the data and return the raw bytes'''
	if sum( 1 for e in s.queue if e.priority == period ) != 0:
		logging.warning(f'Duplicate event found for {gateway[0]}:{gateway[1]}/{period}')
		return	# If there is another event scheduled which does the same as this one, just stop now and let the other one run.
	host, port = gateway
	try:
		with ParamsLock:
			parameters = ParamsByLoggers[gateway][period]	# Dictionary of paramID keys and modbus detail values
	except KeyError:
		logging.debug(f"Fetch for {host}:{port}/{period} cancelled as it's no longer required")
		return	# If, when it comes to it, the gateway or period no longer exist in the list, dont run, and therefore don't schedule it again.
	logging.debug(f'Fetching Modbus data for {host}:{port}/{period}')
	master = modbus_tcp.TcpMaster(host=host, port=port)	# Sets up a TCP connection to the given slave
	results = []
	for id in parameters:	# Polls each device for the data requested
		item = parameters[id]
		param = (item['address'], item['function'], item['register'], item['count'])
		try:
			if param in cache and cache[param][0] == now:	# Check if this parameter has already been captured for this time by a coinciding period
				result = cache[param][1]	# Use the previous version instead
				logging.info(f'Using Cached data for item {id} at {int(now)}')
			else:
				result = master.execute(*param)	# Stores the bytestream as received
				result = struct.pack(f'>{len(result)}H',*result)	# Reencode it to a raw bytestream
				cache[param]=(now,result)	# Cache it in case a subsequent request is the same
				logging.info(f'Successfully polled device for item {id} at {int(now)}')
			results.append({ 'id':id, 'result':result , 'timestamp':int(now)})
		except (ModbusError, ModbusFunctionNotSupportedError, DuplicatedKeyError, MissingKeyError, InvalidModbusBlockError, InvalidArgumentError, OverlapModbusBlockError, OutOfModbusBlockError, ModbusInvalidResponseError, ModbusInvalidRequestError) as e:	# Catch Modbus Specific Exceptions, likely invalid registers etc.
			logging.error(f'Modbus Error with item {id}.  {e}')
			continue	# Work on the next parameter as it may just be this particular slave that has an issue
		except struct.error as e:	# Catch Modbus Specific Exceptions, likely invalid registers etc.
			logging.error(f'Decoding Error with item {id}.  {e}')
			continue	# Work on the next parameter as it may just be this particular slave that has an issue
		except OSError as e:	# Catches network errors trying to connect to the gateway
			logging.error(f'Network Error connecting to {host}:{port}, {e}')
			break	# Break out of the loop as no more parameters can be requested if the gateway itself doesn't work
	master._do_close()	# Closes the connection again
	logging.debug(f'Finished {host}:{port}/{period}')
	Q.put( tuple(results) )	# Upload a tuple of tuples for each individual parameter that was requested to the queue
	nexttime = utcnow()//period*period+period
	s.enterabs(nexttime,period,ModbusRaw,(cache, Q,s,gateway,period,nexttime))	# the period is also the priority

def FetchJobs(sqllock, sql, ParamsByLoggers={}):
	logging.debug('Fetching list of data to be captured from database')
	with sqllock, sql:
		cur =  sql.execute('SELECT host, port, period, paramID AS "id", address, function, register, count FROM RequestedData WHERE required > 0 ORDER BY paramID ASC')
		RequestedData = cur.fetchall()
		keys=[ i[0] for i in cur.description ]	# These are the table headings in the order of the sql select
	logging.debug(f'{len(RequestedData)} parameters were requested within database')
	ParamsByLoggers.clear()	# Clear the Requested list so it can be rebuilt
	for row in RequestedData:
		gateway = tuple( [row['host'], row['port']] )
		period = row['period']
		id = row['id']
		if gateway in ParamsByLoggers:
			if period in ParamsByLoggers[gateway]:
				ParamsByLoggers[gateway][period][id] = dict(zip(keys[4:],row[4:]))
			else:
				ParamsByLoggers[gateway][period] = { id : dict(zip(keys[4:],row[4:])) }
		else:
			ParamsByLoggers[gateway] = { period : { id : dict(zip(keys[4:],row[4:])) } }
	logging.info(f'{len(RequestedData)} parameters were requested accross {len(ParamsByLoggers)} modbus gateways')
	return ParamsByLoggers

def AssignThreads(ParamsLock, ParamsByLoggers, threads, watchdogperiod):
	logging.debug('Assigning workload to threads')
	with ParamsLock:
		for gateway in set(threads) - set(ParamsByLoggers):	# Iterates over all gateways which may have had a ThreadSched which are no longer in the required list of parameters. They should self terminate themselves.
			if not threads[gateway].thread.is_alive():	# Check if the threads have stopped.
				if threads[gateway].sched.empty():
					logging.warning('Removing redundant thread for gateway {}:{}'.format(*gateway))
					del threads[gateway]	# If the thread has stopped and the schedule is empty, then delete it. Its now redundant
				else:	# However if the thread had stopped but there is still stuff in the schedule, restart the thread. This can happen if one schedule raised an exception (and deletes itself in the process) while other schedules remain
					threads[gateway].newthread()	# Create the new Thread to run the existing sched
					threads[gateway].thread.start()	# Start the new thread
		for gateway in ParamsByLoggers:
			if gateway not in threads: 	# Keep the Thread and Schedule objects handy. The Thread simply runs the scheduler and any events within. If the schedule empties, the thread will terminate.
				logging.info('Creating a new schedule of work for gateway {}:{}'.format(*gateway))
				threads[gateway] = ThreadSched(gateway, watchdogperiod)	# Create new ThreadSched Combo
			for period in set( ParamsByLoggers[gateway] ) - threads[gateway].periods():	# Iterates over periods requested which are not already scheduled
				nexttime = utcnow()//period*period+period
				threads[gateway].sched.enterabs(nexttime,period,ModbusRaw,(threads[gateway].cache, Results, threads[gateway].sched, gateway, period, nexttime))
				logging.debug('Job {}:{}/{} scheduled'.format(*gateway,period))
			if not threads[gateway].thread.is_alive():
				try:
					threads[gateway].thread.start()	# If the thread isn't running, replace it and restart it
					logging.info('Started thread for gateway {}:{}'.format(*gateway))
				except RuntimeError:
					logging.error('Thread for {}:{} had already been started before but wasnt alive.'.format(*gateway))
	logging.debug('All work assigned')
	return threads

if __name__ == '__main__':
	starttime = datetime.now(tz=timezone.utc)
	config = SafeConfigParser(defaults={'dbpath':'Logging.db', 'logginglevel':'WARNING', 'watchdogperiod':'1', 'datathreadperiod':'60'},empty_lines_in_values=False)
	with open(join(dirname(realpath(__file__)),'config.cfg'), 'r+') as configfile:
		config.read_file(configfile)
		if not config.has_section('RUNTIME'):
			config.add_section('RUNTIME')
		config.set('RUNTIME','starttime',starttime.isoformat())
		config.set('RUNTIME','pid',str(getpid()))
		configfile.seek(0)
		config.write(configfile)
		configfile.truncate()
	logging.basicConfig(level=config.get('DEFAULT','logginglevel'), format='%(asctime)s : %(levelname)s : %(threadName)s : %(message)s')
	sqllock = threading.Lock()
	sql=sqlite3.connect(config.get('DEFAULT','dbpath'), check_same_thread=False)
	sql.row_factory = sqlite3.Row
	with sqllock, sql:
		sql.executescript('''
			CREATE TABLE IF NOT EXISTS RequestedData(
			paramID INTEGER UNIQUE PRIMARY KEY NOT NULL,
			required INTEGER NOT NULL CHECK (required = 0 OR required = 1),
			host TEXT NOT NULL,
			port INTEGER NOT NULL CHECK (port >= 1 AND port <= 65535),
			period INTEGER NOT NULL CHECK (period > 0),
			address INTEGER NOT NULL CHECK (address >= 1 AND address <= 247),
			function INTEGER NOT NULL CHECK (function >= 1 AND function <= 255),
			register INTEGER NOT NULL CHECK (register >= 0 AND register <= 65535),
			count INTEGER NOT NULL CHECK (count >= 1 AND count <= 126),
			comment TEXT,
			UNIQUE (host,port,period,address,function,register,count)
			);
			CREATE TABLE IF NOT EXISTS ResultsRaw (
			paramID INTEGER NOT NULL,
			timestamp INTEGER NOT NULL CHECK (timestamp >= 0),
			rawresult BLOB,
			FOREIGN KEY (paramID) REFERENCES RequestedData(paramID) ON DELETE CASCADE ON UPDATE CASCADE,
			UNIQUE (paramID,timestamp)
			);
			CREATE UNIQUE INDEX IF NOT EXISTS ResultIdx ON ResultsRaw
			(paramID ASC, timestamp ASC)
			;''')
	Shutdown = threading.Event()	# Setting this will cause all threads to eventually terminate
	Results = queue.Queue()	# Logger threads will push raw results to here and Storers will consume items and put them into the database
	DataSched = sched.scheduler()
	ParamsLock, ParamsByLoggers, threads = threading.Lock(), {} , {}
	DataSched.enter(0,0,DataWatchdog,(DataSched,config.getfloat('DEFAULT','watchdogperiod')))
	DataSched.enter(0,0,FetchAndStore,(sqllock, sql, Results, DataSched, config.getfloat('DEFAULT','datathreadperiod'),config.getfloat('DEFAULT','watchdogperiod')))
	Storer = threading.Thread(target=DataSched.run, name='DataThread')
	Storer.start()	# Start a thread for data storage
	signal.signal(signal.SIGTERM,TerminateSignal)
	try:
		while not Shutdown.is_set():
			if not isfile(join(dirname(realpath(__file__)),f'{getpid()}.stop')):
				time.sleep(config.getfloat('DEFAULT','watchdogperiod'))
			else:
				logging.warning('Found the stop file. Initiating Shutdown')
				Shutdown.set()
				with ParamsLock:
					ParamsByLoggers.clear()
	except KeyboardInterrupt:
		logging.warning('Ctrl+C caught. Initiating Shutdown')
		Shutdown.set()
		with ParamsLock:
			ParamsByLoggers.clear()
	for t in threads:	# All threads should eventually terminate and so this should return soon enough
		threads[t].thread.join()
	logging.info('All data retreival threads have stopped')
	Storer.join()	# Should this should also have terminated by now
	logging.info('DataThread has stopped')
	Results.join()	# This should be empty so should return immediately
	logging.info('Results Queue is empty')
	logging.warning('Terminating')
	raise SystemExit