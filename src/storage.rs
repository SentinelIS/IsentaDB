use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

pub const PAGE_SIZE: usize = 4096;

pub struct Page {
    pub id: u64,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            data: [0; PAGE_SIZE],
        }
    }
}

pub struct StorageEngine {
    file: File,
}

impl StorageEngine {
    /// Opens or creates the databse-file
    pub fn new(path: &str) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Could not open database file");

        Self { file }
    }

    /// Reads one page with given ID
    /// Returns a zero-filled page if the page doesn't exist yet
    pub fn read_page(&mut self, page_id: u64) -> Page {
        let mut page = Page::new(page_id);

        let offset = page_id * PAGE_SIZE as u64;
        let file_len = self.file.metadata().unwrap().len();
        
        // If the page is beyond the file, return zero-filled page
        if offset >= file_len {
            return page;
        }

        self.file
            .seek(SeekFrom::Start(offset))
            .expect("Seek failed");

        // Read as much as we can, rest will be zeros
        match self.file.read(&mut page.data) {
            Ok(bytes_read) => {
                // If we didn't read a full page, the rest is already zero-filled
                if bytes_read < PAGE_SIZE {
                    // Clear any remaining bytes (though they should already be zero)
                    for i in bytes_read..PAGE_SIZE {
                        page.data[i] = 0;
                    }
                }
            }
            Err(_) => {
                // On error, return zero-filled page
                // This handles cases where the file is truncated or corrupted
            }
        }

        page
    }

    /// Writes a Page
    pub fn write_page(&mut self, page: &Page) {
        let offset = page.id * PAGE_SIZE as u64;

        self.file
            .seek(SeekFrom::Start(offset))
            .expect("Seek failed");

        self.file
            .write_all(&page.data)
            .expect("Failed to write page");

        self.file.flush().unwrap();
    }

    /// Creates a new Page a the end of file
    pub fn allocate_page(&mut self) -> Page {
        let file_len = self.file.metadata().unwrap().len();
        let next_page_id = file_len / PAGE_SIZE as u64;

        let page = Page::new(next_page_id);
        self.write_page(&page);

        page
    }

    /// Get file metadata
    pub fn file(&mut self) -> &mut File {
        &mut self.file
    }
}
