CREATE OR REPLACE FUNCTION notify_activity_insert()
RETURNS trigger AS $$
BEGIN
    IF NEW.severity = 1 OR NEW.send_alert = TRUE THEN
        PERFORM pg_notify('activity_update', NEW.activity_id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger on the activity table
CREATE TRIGGER activity_insert_notify
AFTER INSERT ON activity
FOR EACH ROW
EXECUTE FUNCTION notify_activity_insert();

-- Update resolve datetime when resolve_initials are filled
CREATE OR REPLACE FUNCTION update_resolve_datetime()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if resolve_initials is being set from NULL to non-NULL or is updated to a different value.
    IF NEW.resolve_initials IS NOT NULL AND (OLD IS NULL OR OLD.resolve_initials IS NULL OR NEW.resolve_initials <> OLD.resolve_initials) THEN
        NEW.resolve_datetime := CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_resolve_datetime_trigger
BEFORE INSERT OR UPDATE ON activity
FOR EACH ROW
EXECUTE FUNCTION update_resolve_datetime();


SELECT cron.schedule(
  'daily_cleanup',
  '0 0 * * *',  -- every day at midnight
  'DELETE FROM activities WHERE severity = 4 and datetime < now() - interval ''30 days'';'
);