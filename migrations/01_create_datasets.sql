CREATE TABLE datasets (
  id UUID PRIMARY KEY,
  filename TEXT NOT NULL,
  storage_path TEXT NOT NULL,
  format TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
