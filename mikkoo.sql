CREATE SCHEMA mikkoo;

CREATE TABLE mikkoo.audit (
    message_id   UUID NOT NULL,
    event_id     BIGINT NOT NULL,
    queue        TEXT NOT NULL,
    exchange     TEXT NOT NULL,
    routing_key  TEXT NOT NULL,
    payload      TEXT NOT NULL,
    content_type TEXT,
    properties   TEXT,
    headers      TEXT,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX audit_message_id ON mikkoo.audit (message_id);

COMMENT ON TABLE mikkoo.audit IS 'Logs all messages published to pgq for auditing purposes.';
COMMENT ON COLUMN mikkoo.audit.message_id IS 'ID for the message that was published';
COMMENT ON COLUMN mikkoo.audit.event_id IS 'The PgQ event ID';
COMMENT ON COLUMN mikkoo.audit.queue IS 'Queue the message was published to';
COMMENT ON COLUMN mikkoo.audit.exchange IS 'AMQP exchange name';
COMMENT ON COLUMN mikkoo.audit.routing_key IS 'AMQP routing Key';
COMMENT ON COLUMN mikkoo.audit.payload IS 'AMQP message payload';
COMMENT ON COLUMN mikkoo.audit.content_type IS 'AMQP content_type message property';
COMMENT ON COLUMN mikkoo.audit.properties IS 'AMQP message properties';
COMMENT ON COLUMN mikkoo.audit.headers IS 'AMQP headers message property';
COMMENT ON COLUMN mikkoo.audit.published_at IS 'When the message was published';

CREATE OR REPLACE FUNCTION mikkoo.delete_audit_record(in_message_id UUID)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE STRICT
AS $BODY$
BEGIN
  DELETE FROM mikkoo.audit WHERE message_id = in_message_id;
  RETURN FOUND;
END;
$BODY$;

COMMENT ON FUNCTION mikkoo.delete_audit_record(uuid) IS '
Delete a row from mikkoo.audit
INPUTS: in_message_id - the message id of the published message
OUTPUTS: FOUND - true if delete was successful, false otherwise.
';

CREATE OR REPLACE FUNCTION mikkoo.new_audit_record(in_message_id UUID, in_event_id BIGINT, in_queue TEXT, in_exchange TEXT, in_routing_key TEXT, in_payload TEXT, in_content_type TEXT, in_properties TEXT, in_headers TEXT)
RETURNS mikkoo.audit
LANGUAGE SQL
SECURITY DEFINER
VOLATILE STRICT
AS $BODY$
  INSERT INTO mikkoo.audit (message_id, event_id, queue, exchange, routing_key, payload, content_type, properties, headers)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING *;
$BODY$;

COMMENT ON FUNCTION mikkoo.new_audit_record(uuid, bigint, text, text, text, text, text, text, text) IS '
Create a new row in mikkoo.audit
INPUTS: in_message_id - the message id of the published message
        in_event_id - the pgq event ID
        in_queue - the queue the message was sent to
        in_exchange - the exchange the message should be published to
        in_routing_key - the routing key the message should be published with
        in_payload - the payload that should have been sent
        in_content_type - AMQP content_type property for the message
        in_properties - AMQP properties for the message
        in_headers - AMQP headers property for the message
OUTPUTS: message_id text - the sequence-generated id of the feature
         event_id - the pgq event ID
         queue - the queue the message was sent to
         exchange - the exchange the message should be published to
         routing_key - the routing key the message should be published with
         payload - the payload that should have been sent
         content_type - AMQP content_type property for the message
         properties - AMQP properties for the message
         headers - AMQP headers property for the message
         published_at - the timestamp from when the message was published';

CREATE OR REPLACE FUNCTION mikkoo.insert_audited_event(in_queue text, in_exchange text, in_routing_key text, in_payload text, in_content_type text)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE STRICT
AS $BODY$
DECLARE
    v_event_id BIGINT;
    v_message_id UUID;
    v_headers TEXT;
    v_properties TEXT;
BEGIN
    SELECT uuid_generate_v4 INTO v_message_id FROM public.uuid_generate_v4();
    v_headers := '{"pgq_queue": "' || in_queue || '"}';
    v_properties := '{"message_id": "' || v_message_id || '"}';
    SELECT insert_event INTO v_event_id FROM pgq.insert_event(in_queue, in_routing_key, in_payload, in_exchange, in_content_type, v_properties, v_headers);
    PERFORM mikkoo.new_audit_record(v_message_id, v_event_id, in_queue, in_exchange, in_routing_key, in_payload, in_content_type, v_properties, v_headers);
    RETURN v_event_id;
END
$BODY$;

COMMENT ON FUNCTION mikkoo.insert_audited_event(text, text, text, text, text)  IS '
Inserts an event into the specified pgq queue and a record into the audit table.
A message_id is automatically generated and set in the AMQP message properties.
The queue name is added to the AMQP headers message property under the key "pgq_queue"

This function requires the uuid-ossp extension.

INPUTS: in_queue - the queue the message was sent to
        in_exchange - the exchange the message should be published to
        in_routing_key - the routing key the message should be published with
        in_payload - the payload that should have been sent
        in_content_type - AMQP content_type property for the message
OUTPUTS: pgq event ID';

CREATE OR REPLACE FUNCTION mikkoo.insert_event(in_queue text, in_exchange text, in_routing_key text, in_payload text, in_content_type text)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE STRICT
AS $BODY$
DECLARE
    v_event_id BIGINT;
    v_message_id UUID;
    v_headers TEXT;
    v_properties TEXT;
BEGIN
    SELECT uuid_generate_v4 INTO v_message_id FROM public.uuid_generate_v4();
    v_headers := '{"pgq_queue": "' || in_queue || '"}';
    v_properties := '{"message_id": "' || v_message_id || '"}';
    SELECT insert_event INTO v_event_id FROM pgq.insert_event(in_queue, in_routing_key, in_payload, in_exchange, in_content_type, v_properties, v_headers);
    RETURN v_event_id;
END
$BODY$;

COMMENT ON FUNCTION mikkoo.insert_event(text, text, text, text, text)  IS '
Inserts an event into the specified pgq queue.
A message_id is automatically generated and set in the AMQP message properties.
The queue name is added to the AMQP headers message property under the key "pgq_queue"

INPUTS: in_queue - the queue the message was sent to
        in_exchange - the exchange the message should be published to
        in_routing_key - the routing key the message should be published with
        in_payload - the payload that should have been sent
        in_content_type - AMQP content_type property for the message
OUTPUTS: pgq event ID';

CREATE OR REPLACE FUNCTION mikkoo.insert_event(in_queue text, in_exchange text, in_routing_key text, in_payload text, in_content_type text, in_properties text, in_headers text)
RETURNS BIGINT
LANGUAGE SQL
SECURITY DEFINER
VOLATILE STRICT
AS $BODY$
    SELECT insert_event FROM pgq.insert_event(in_queue, in_routing_key, in_payload, in_exchange, in_content_type, in_properties, in_headers);
$BODY$;

COMMENT ON FUNCTION mikkoo.insert_event(text, text, text, text, text, text, text)  IS '
Inserts an event into the specified pgq queue.

INPUTS: in_queue - the queue the message was sent to
        in_exchange - the exchange the message should be published to
        in_routing_key - the routing key the message should be published with
        in_payload - the payload that should have been sent
        in_content_type - AMQP content_type property for the message
        in_properties - AMQP message properties
        in_headers - AMQP headers message property
OUTPUTS: pgq event ID';
