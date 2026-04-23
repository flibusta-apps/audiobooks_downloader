PRAGMA foreign_keys = ON;

CREATE TABLE downloads (
    id TEXT PRIMARY KEY NOT NULL,
    magnet_uri TEXT NOT NULL,
    torrent_name TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending_metadata', 'downloading', 'completed', 'failed', 'interrupted', 'expired')),
    storage_dir TEXT NOT NULL,
    total_bytes INTEGER,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0 CHECK (downloaded_bytes >= 0),
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    metadata_at TEXT,
    completed_at TEXT,
    expires_at TEXT
);

CREATE INDEX idx_downloads_status ON downloads(status);
CREATE INDEX idx_downloads_expires_at ON downloads(expires_at);

CREATE TABLE download_files (
    download_id TEXT NOT NULL,
    file_index INTEGER NOT NULL CHECK (file_index >= 0),
    relative_path TEXT NOT NULL CHECK (relative_path <> ''),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    mime_type TEXT NOT NULL CHECK (mime_type <> ''),
    PRIMARY KEY (download_id, file_index),
    FOREIGN KEY (download_id) REFERENCES downloads(id) ON DELETE CASCADE
);
