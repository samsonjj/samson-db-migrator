use chrono::Utc;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct FileData {
    pub checksum: i32,
    pub filename: String,
    pub path: PathBuf,
    pub ts: chrono::DateTime<chrono::Utc>,
}

impl FileData {
    pub fn get_map(paths: &Vec<PathBuf>) -> HashMap<String, Self> {
        paths
            .iter()
            .map(|path| {
                (
                    path.file_name().unwrap().to_str().unwrap().to_owned(),
                    FileData {
                        checksum: 0,
                        filename: path
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_owned(),
                        path: path.clone(),
                        ts: Utc::now(),
                    },
                )
            })
            .collect()
    }
}
