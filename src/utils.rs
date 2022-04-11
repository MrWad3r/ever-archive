use std::fs::File;

pub struct FileView<'a> {
    _file: &'a File,
    length: usize,
    ptr: *mut libc::c_void,
}

impl<'a> FileView<'a> {
    pub fn new(file: &'a File) -> std::io::Result<Self> {
        use std::os::unix::io::AsRawFd;

        let length = file.metadata()?.len() as usize;

        // SAFETY: File was opened successfully, file mode is R, offset is aligned
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        if unsafe { libc::madvise(ptr, length, libc::MADV_SEQUENTIAL) } != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self {
            _file: file,
            length,
            ptr,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.length) }
    }
}

impl Drop for FileView<'_> {
    fn drop(&mut self) {
        // SAFETY: File still exists, ptr and length were initialized once on creation
        if unsafe { libc::munmap(self.ptr, self.length) } != 0 {
            // TODO: how to handle this?
            let error = std::io::Error::last_os_error();
            panic!("failed to unmap temp archive file: {error}");
        }
    }
}
