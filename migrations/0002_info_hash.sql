ALTER TABLE downloads ADD COLUMN info_hash TEXT;

CREATE INDEX idx_downloads_info_hash ON downloads(info_hash);
