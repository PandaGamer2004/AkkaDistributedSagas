 CREATE TABLE IF NOT EXISTS events (
       id SERIAL PRIMARY KEY,
       channelid UUID NOT NULL,
       eventdata TEXT NOT NULL
 );
 
 DO $$
 BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_channelid') THEN
          EXECUTE 'CREATE INDEX idx_channelid ON events (channelid)';
      END IF;
 END $$;