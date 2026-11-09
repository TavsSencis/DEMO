//! [DATA PLANE] Minimal zero-allocation scanning of JSON messages whose shape the caller knows.
//! Not a general parser: venue modules use it on their own message layouts, where keys are
//! unique enough to find by name and string values contain no escaped quotes.

/// Index just after `"key":` (and any whitespace), searching from the start.
#[inline]
fn value_start(json: &[u8], key: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i + key.len() + 2 < json.len() {
        if json[i] == b'"' && json[i + 1..].starts_with(key) && json[i + 1 + key.len()] == b'"' {
            let mut j = i + key.len() + 2;
            while j < json.len() && json[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < json.len() && json[j] == b':' {
                j += 1;
                while j < json.len() && json[j].is_ascii_whitespace() {
                    j += 1;
                }
                return Some(j);
            }
        }
        i += 1;
    }
    None
}

/// Contents of `"key":"value"` (without quotes).
#[inline]
pub fn str_field<'a>(json: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let start = value_start(json, key)?;
    if json.get(start) != Some(&b'"') {
        return None;
    }
    let body = &json[start + 1..];
    let end = body.iter().position(|&c| c == b'"')?;
    Some(&body[..end])
}

/// Raw token of `"key":<number|true|false|null>`.
#[inline]
pub fn raw_field<'a>(json: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let start = value_start(json, key)?;
    let body = &json[start..];
    let end = body
        .iter()
        .position(|&c| matches!(c, b',' | b'}' | b']') || c.is_ascii_whitespace())
        .unwrap_or(body.len());
    (end > 0).then(|| &body[..end])
}

/// `"key":{...}` or `"key":[...]`, brackets included. Skips brackets inside strings.
#[inline]
pub fn container_field<'a>(json: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let start = value_start(json, key)?;
    let open = *json.get(start)?;
    let close = match open {
        b'{' => b'}',
        b'[' => b']',
        _ => return None,
    };
    let mut depth = 0usize;
    let mut in_str = false;
    let mut escaped = false;
    for (k, &c) in json[start..].iter().enumerate() {
        if in_str {
            match c {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_str = false,
                _ => {}
            }
            continue;
        }
        match c {
            b'"' => in_str = true,
            _ if c == open => depth += 1,
            _ if c == close => {
                depth -= 1;
                if depth == 0 {
                    return Some(&json[start..=start + k]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Iterates the first two strings of each inner array of `[["a","b",...],["c","d",...]]`.
pub struct StrPairs<'a> {
    rest: &'a [u8],
}

impl<'a> StrPairs<'a> {
    /// `array` is the outer array including its brackets (e.g. from `container_field`).
    #[inline]
    pub fn new(array: &'a [u8]) -> Self {
        Self { rest: array.get(1..).unwrap_or(&[]) }
    }

    #[inline(always)]
    fn next_string(&mut self) -> Option<&'a [u8]> {
        let open = self.rest.iter().position(|&c| c == b'"')?;
        let body = &self.rest[open + 1..];
        let close = body.iter().position(|&c| c == b'"')?;
        self.rest = &body[close + 1..];
        Some(&body[..close])
    }
}

impl<'a> Iterator for StrPairs<'a> {
    type Item = (&'a [u8], &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let open = self.rest.iter().position(|&c| c == b'[')?;
        self.rest = &self.rest[open + 1..];
        let a = self.next_string()?;
        let b = self.next_string()?;
        let close = self.rest.iter().position(|&c| c == b']')?;
        self.rest = &self.rest[close + 1..];
        Some((a, b))
    }
}
