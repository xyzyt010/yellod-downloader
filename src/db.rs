use rusqlite::{Connection, params};
use serde::{Serialize, Deserialize};
use std::sync::Mutex;
use chrono::Utc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub source_url: String,
    pub title: Option<String>,
    pub thumbnail_url: Option<String>,
    pub media_type: String,
    pub quality: String,
    pub language: Option<String>,
    pub tool: String,
    pub status: String,
    pub progress: f64,
    pub file_path: Option<String>,
    pub file_size: Option<i64>,
    pub error_message: Option<String>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
    /// "direct" | "tor" | "proxy" — which network route the download used.
    #[serde(default)]
    pub route: Option<String>,
}

pub struct Database {
    pub conn: Mutex<Connection>,
}

impl Database {
    pub fn new(path: &str) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS jobs (
                id            TEXT PRIMARY KEY,
                source_url    TEXT NOT NULL,
                title         TEXT,
                thumbnail_url TEXT,
                media_type    TEXT NOT NULL,
                quality       TEXT NOT NULL,
                language      TEXT,
                tool          TEXT NOT NULL,
                status        TEXT NOT NULL,
                progress      REAL DEFAULT 0,
                file_path     TEXT,
                file_size     INTEGER,
                error_message TEXT,
                created_at    INTEGER NOT NULL,
                completed_at  INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at DESC);"
        )?;
        // Migration: older databases do not have the `route` column yet.
        let has_route = conn
            .prepare("PRAGMA table_info(jobs)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .filter_map(Result::ok)
            .any(|name| name == "route");
        if !has_route {
            conn.execute_batch("ALTER TABLE jobs ADD COLUMN route TEXT;")?;
        }
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn insert_job(&self, job: &Job) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO jobs (id, source_url, title, thumbnail_url, media_type, quality, language, tool, status, progress, file_path, file_size, error_message, created_at, completed_at, route) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![job.id, job.source_url, job.title, job.thumbnail_url, job.media_type, job.quality, job.language, job.tool, job.status, job.progress, job.file_path, job.file_size, job.error_message, job.created_at, job.completed_at, job.route],
        )?;
        Ok(())
    }

    pub fn update_job_status(&self, id: &str, status: &str, progress: f64, file_path: Option<&str>, file_size: Option<i64>, error_message: Option<&str>) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let completed_at = if status == "done" || status == "error" || status == "cancelled" {
            Some(Utc::now().timestamp())
        } else {
            None
        };
        conn.execute(
            "UPDATE jobs SET status=?1, progress=?2, file_path=COALESCE(?3, file_path), file_size=COALESCE(?4, file_size), error_message=COALESCE(?5, error_message), completed_at=COALESCE(?6, completed_at) WHERE id=?7",
            params![status, progress, file_path, file_size, error_message, completed_at, id],
        )?;
        Ok(())
    }

    pub fn get_job(&self, id: &str) -> Result<Option<Job>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source_url, title, thumbnail_url, media_type, quality, language, tool, status, progress, file_path, file_size, error_message, created_at, completed_at, route FROM jobs WHERE id=?1"
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Job {
                id: row.get(0)?,
                source_url: row.get(1)?,
                title: row.get(2)?,
                thumbnail_url: row.get(3)?,
                media_type: row.get(4)?,
                quality: row.get(5)?,
                language: row.get(6)?,
                tool: row.get(7)?,
                status: row.get(8)?,
                progress: row.get(9)?,
                file_path: row.get(10)?,
                file_size: row.get(11)?,
                error_message: row.get(12)?,
                created_at: row.get(13)?,
                completed_at: row.get(14)?,
                route: row.get(15)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn list_jobs(&self, limit: i64) -> Result<Vec<Job>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source_url, title, thumbnail_url, media_type, quality, language, tool, status, progress, file_path, file_size, error_message, created_at, completed_at, route FROM jobs ORDER BY created_at DESC LIMIT ?1"
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            Ok(Job {
                id: row.get(0)?,
                source_url: row.get(1)?,
                title: row.get(2)?,
                thumbnail_url: row.get(3)?,
                media_type: row.get(4)?,
                quality: row.get(5)?,
                language: row.get(6)?,
                tool: row.get(7)?,
                status: row.get(8)?,
                progress: row.get(9)?,
                file_path: row.get(10)?,
                file_size: row.get(11)?,
                error_message: row.get(12)?,
                created_at: row.get(13)?,
                completed_at: row.get(14)?,
                route: row.get(15)?,
            })
        })?;
        rows.collect()
    }

    pub fn set_job_route(&self, id: &str, route: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("UPDATE jobs SET route=?1 WHERE id=?2", params![route, id])?;
        Ok(())
    }

    /// Set the display title only when the job does not have one yet
    /// (the UI-supplied title is nicer than the raw filename).
    pub fn set_title_if_empty(&self, id: &str, title: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET title=?1 WHERE id=?2 AND (title IS NULL OR title='')",
            params![title, id],
        )?;
        Ok(())
    }

    /// Persist the media's thumbnail (YouTube cover art) when the job was
    /// queued without one - covered pastes / playlists / direct API calls -
    /// so history shows the cover photo. Never overwrites a real one.
    pub fn set_thumbnail_if_empty(&self, id: &str, thumbnail: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET thumbnail_url=?1 WHERE id=?2 AND (thumbnail_url IS NULL OR thumbnail_url='')",
            params![thumbnail, id],
        )?;
        Ok(())
    }

    pub fn delete_job(&self, id: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM jobs WHERE id=?1", params![id])?;
        Ok(())
    }

    pub fn all_job_ids(&self) -> Result<Vec<String>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id FROM jobs")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn delete_jobs(&self, ids: &[String]) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        for id in ids {
            conn.execute("DELETE FROM jobs WHERE id=?1", params![id])?;
        }
        Ok(())
    }

    pub fn delete_all_jobs(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM jobs", params![])?;
        Ok(())
    }

    pub fn cleanup_old_jobs(&self, ttl_hours: u64) -> Result<usize, rusqlite::Error> {
        let cutoff = Utc::now().timestamp() - (ttl_hours as i64 * 3600);
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute("DELETE FROM jobs WHERE completed_at IS NOT NULL AND completed_at < ?1", params![cutoff])?;
        Ok(deleted)
    }
}
