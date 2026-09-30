CREATE TABLE events (
  id uuid PRIMARY KEY,
  occurred_at timestamptz NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  type text NOT NULL CHECK (type IN ('click', 'view')),
  target_id text NOT NULL CHECK (char_length(target_id) BETWEEN 1 AND 512)
);

CREATE INDEX events_occurred_at_idx ON events (occurred_at);
CREATE INDEX events_type_occurred_at_idx ON events (type, occurred_at);
