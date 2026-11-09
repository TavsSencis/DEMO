//! [DATA PLANE] Decimal text -> fixed-point integer. Zero allocation; for venues that send numbers as text.

/// Parses `[-]digits[.digits]` into a mantissa at exponent `exp` (value = mantissa * 10^exp,
/// `exp <= 0`). Returns `None` for empty or malformed input, overflow, or non-zero digits
/// beyond the scale (precision is never silently dropped). Trailing zeros are accepted.
#[inline]
pub fn parse_fixed(s: &[u8], exp: i8) -> Option<i64> {
    if exp > 0 {
        return None;
    }
    let scale = (-(exp as i32)) as u32;
    let (neg, digits) = match s.first() {
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    let mut mantissa: i64 = 0;
    let mut frac_digits = 0u32;
    let mut seen_dot = false;
    let mut any_digit = false;
    for &c in digits {
        match c {
            b'0'..=b'9' => {
                any_digit = true;
                if seen_dot {
                    if frac_digits == scale {
                        if c != b'0' {
                            return None;
                        }
                        continue;
                    }
                    frac_digits += 1;
                }
                mantissa = mantissa.checked_mul(10)?.checked_add((c - b'0') as i64)?;
            }
            b'.' if !seen_dot => seen_dot = true,
            _ => return None,
        }
    }
    if !any_digit {
        return None;
    }
    mantissa = mantissa.checked_mul(10i64.checked_pow(scale - frac_digits)?)?;
    Some(if neg { -mantissa } else { mantissa })
}

/// Parses an unsigned decimal integer (`"1712100000000000"` or raw `296050733`).
#[inline]
pub fn parse_u64(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    Some(v)
}
