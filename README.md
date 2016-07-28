# rds_log_streamer

Requires python 2.7.x and boto3.


Stream log lines from multiple RDS instances to STDOUT.

    usage: rds_log_streamer.py [-h] --db_instance_ids DB_INSTANCE_IDS
                               [DB_INSTANCE_IDS ...]
                               [--minutes_in_the_past_to_start MINUTES_IN_THE_PAST_TO_START]
                               [--api_call_delay_seconds API_CALL_DELAY_SECONDS]
                               [--log_state_file LOG_STATE_FILE]
                               [--retention_days RETENTION_DAYS]
                               [--log_level {DEBUG,INFO,WARN,ERROR,CRITICAL}]
                               [--log_filename LOG_FILENAME] [--run_once]
    
    Stream logs from rds for a set of db instances.
    
    optional arguments:
      -h, --help            show this help message and exit
      --db_instance_ids DB_INSTANCE_IDS [DB_INSTANCE_IDS ...], -d DB_INSTANCE_IDS [DB_INSTANCE_IDS ...]
                            list of db instance ids
      --minutes_in_the_past_to_start MINUTES_IN_THE_PAST_TO_START, -m MINUTES_IN_THE_PAST_TO_START
                            if logs have not been written to since this many
                            minutes ago, ignore them
      --api_call_delay_seconds API_CALL_DELAY_SECONDS, -a API_CALL_DELAY_SECONDS
                            time to wait before each API call
      --log_state_file LOG_STATE_FILE, -s LOG_STATE_FILE
                            file path for recording the state of log streaming
      --retention_days RETENTION_DAYS, -r RETENTION_DAYS
                            number of days to retain log metadata
      --log_level {DEBUG,INFO,WARN,ERROR,CRITICAL}, -l {DEBUG,INFO,WARN,ERROR,CRITICAL}
                            log level for this script's logs
      --log_filename LOG_FILENAME, -f LOG_FILENAME
                            log filename for this script's logs
      --run_once, -o        stream all new logs from all db instances and then
                            exit

  
