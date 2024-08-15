pub(super) fn range_end_for_prefix(prefix: &[u8]) -> Vec<u8> {
    // Start with the same bytes as the prefix
    let mut end = Vec::from(prefix);

    // Iterate from the end of the prefix to the start
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            // Increment the current byte by 1
            end[i] += 1;
            // Truncate the vector to this point
            end.truncate(i + 1);
            return end;
        }
    }

    // If all bytes were 0xff, append a 0x00 byte to the end
    end.push(0x00);
    end
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_end_basic() {
        assert_eq!(range_end_for_prefix(b"a"), b"b");
    }

    #[test]
    fn test_range_end_complex() {
        assert_eq!(range_end_for_prefix(b"ab"), b"ac");
        assert_eq!(range_end_for_prefix(b"a\xff"), b"b");
    }

    #[test]
    fn test_range_end_all_ff() {
        assert_eq!(range_end_for_prefix(b"\xff\xff"), b"\xff\xff\x00");
    }

    #[test]
    fn test_range_end_empty() {
        assert_eq!(range_end_for_prefix(b""), b"\x00");
    }
}
