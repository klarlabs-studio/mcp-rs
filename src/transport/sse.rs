#[derive(Debug, thiserror::Error)]
pub(crate) enum SseError {
    #[error("SSE event size limit exceeded")]
    Limit,
    #[error("invalid SSE: {0}")]
    Invalid(&'static str),
}

pub(crate) struct Event {
    pub name: String,
    pub data: String,
}
pub(crate) struct Decoder {
    line: Vec<u8>,
    name: String,
    data: String,
    has_data: bool,
    bytes: usize,
    limit: usize,
    skip_lf: bool,
}
impl Decoder {
    pub fn new(limit: usize) -> Self {
        Self {
            line: vec![],
            name: String::new(),
            data: String::new(),
            has_data: false,
            bytes: 0,
            limit,
            skip_lf: false,
        }
    }
    pub fn push(&mut self, byte: u8) -> Result<Option<Event>, SseError> {
        if self.skip_lf {
            self.skip_lf = false;
            if byte == b'\n' {
                return Ok(None);
            }
        }
        self.bytes = self.bytes.checked_add(1).ok_or(SseError::Limit)?;
        if self.bytes > self.limit {
            return Err(SseError::Limit);
        }
        if byte != b'\r' && byte != b'\n' {
            self.line.push(byte);
            return Ok(None);
        }
        self.skip_lf = byte == b'\r';
        let line = String::from_utf8(std::mem::take(&mut self.line))
            .map_err(|_| SseError::Invalid("SSE is not UTF-8"))?;
        if line.is_empty() {
            self.bytes = 0;
            let name = std::mem::take(&mut self.name);
            let mut data = std::mem::take(&mut self.data);
            if !std::mem::take(&mut self.has_data) {
                return Ok(None);
            }
            data.pop(); // remove the data-line terminator, preserving empty data
            return Ok(Some(Event { name, data }));
        }
        if line.starts_with(':') {
            return Ok(None);
        }
        let (field, value) = line.split_once(':').unwrap_or((&line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => self.name = value.to_owned(),
            "data" => {
                self.has_data = true;
                self.data.push_str(value);
                self.data.push('\n');
            }
            _ => {}
        }
        Ok(None)
    }
}
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    #[test]
    fn chunk_boundaries_crlf_comments_multiline_and_empty_events() {
        let mut decoder = Decoder::new(1024);
        let mut events = vec![];
        for byte in b":keepalive\r\nevent: message\r\ndata: first\r\ndata:second\r\n\r\ndata:\n\n" {
            if let Some(event) = decoder.push(*byte).unwrap() {
                events.push(event);
            }
        }
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].name, "message");
        assert_eq!(events[0].data, "first\nsecond");
        assert_eq!(events[1].data, "");
    }
    #[test]
    fn bounds_incomplete_lines_and_event_data_and_rejects_bad_utf8() {
        for input in [b"data: abcdefghijk".as_slice(), b"data:a\ndata:b\ndata:c\n"] {
            let mut decoder = Decoder::new(12);
            assert!(input.iter().any(|byte| decoder.push(*byte).is_err()));
        }
        let mut decoder = Decoder::new(32);
        assert!(
            b"data:\xff\n"
                .iter()
                .any(|byte| decoder.push(*byte).is_err())
        );
    }
}
