//! Read-only agenda from an iCalendar feed.
//!
//! The token, when one is configured, stays in the environment. This module
//! only parses the text the caller already fetched.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgendaItem {
    pub start: String,
    pub summary: String,
}

pub fn wants_agenda(query: &str) -> bool {
    let q = query.to_ascii_lowercase();
    const STEMS: &[&str] = &[
        "calendar",
        "agenda",
        "schedule",
        "meeting",
        "tomorrow",
        "today",
        "календар",
        "завтра",
        "сегодня",
        "встреч",
    ];
    STEMS.iter().any(|stem| q.contains(stem))
}

/// Events whose start falls inside `[now_ms, now_ms + horizon_ms]`.
pub fn upcoming(ics: &str, now_ms: i64, horizon_ms: i64) -> Vec<AgendaItem> {
    let mut items = Vec::new();
    let mut start: Option<(i64, String)> = None;
    let mut summary = String::new();
    let mut in_event = false;
    for raw in unfold(ics) {
        let line = raw.trim();
        if line.eq_ignore_ascii_case("BEGIN:VEVENT") {
            in_event = true;
            start = None;
            summary.clear();
            continue;
        }
        if line.eq_ignore_ascii_case("END:VEVENT") {
            if let Some((ms, label)) = start.take() {
                if in_event && ms >= now_ms && ms <= now_ms.saturating_add(horizon_ms) {
                    items.push(AgendaItem {
                        start: label,
                        summary: if summary.is_empty() {
                            "(no title)".into()
                        } else {
                            summary.clone()
                        },
                    });
                }
            }
            in_event = false;
            continue;
        }
        if !in_event {
            continue;
        }
        let Some((name, value)) = split_prop(line) else {
            continue;
        };
        if name.eq_ignore_ascii_case("SUMMARY") {
            summary = value;
        } else if name.to_ascii_uppercase().starts_with("DTSTART") {
            if let Some(parsed) = parse_dt(&value) {
                start = Some(parsed);
            }
        }
    }
    items.sort_by(|a, b| a.start.cmp(&b.start));
    items.truncate(12);
    items
}

pub fn format_agenda(items: &[AgendaItem]) -> String {
    if items.is_empty() {
        return "No events in the configured window.".into();
    }
    let mut out = String::from("Upcoming:\n");
    for item in items {
        out.push_str(&format!("- {} {}\n", item.start, item.summary));
    }
    out
}

fn unfold(ics: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for line in ics.lines() {
        if (line.starts_with(' ') || line.starts_with('\t')) && !lines.is_empty() {
            let last = lines.len() - 1;
            lines[last].push_str(line.trim_start());
        } else {
            lines.push(line.trim_end_matches('\r').to_string());
        }
    }
    lines
}

fn split_prop(line: &str) -> Option<(String, String)> {
    let (left, value) = line.split_once(':')?;
    let name = left.split(';').next().unwrap_or(left);
    Some((name.to_string(), unescape(value)))
}

fn unescape(value: &str) -> String {
    value
        .replace("\\n", " ")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
}

fn parse_dt(value: &str) -> Option<(i64, String)> {
    let v = value.trim();
    let digits: String = v.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 8 {
        return None;
    }
    let year: i64 = digits[0..4].parse().ok()?;
    let month: i64 = digits[4..6].parse().ok()?;
    let day: i64 = digits[6..8].parse().ok()?;
    let (hour, min, sec) = if digits.len() >= 14 {
        (
            digits[8..10].parse().unwrap_or(0),
            digits[10..12].parse().unwrap_or(0),
            digits[12..14].parse().unwrap_or(0),
        )
    } else {
        (0, 0, 0)
    };
    let ms = civil_to_unix_ms(year, month, day, hour, min, sec)?;
    let label = if digits.len() >= 14 {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{min:02}Z")
    } else {
        format!("{year:04}-{month:02}-{day:02}")
    };
    Some((ms, label))
}

fn civil_to_unix_ms(year: i64, month: i64, day: i64, hour: i64, min: i64, sec: i64) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut y = year;
    let mut m = month;
    if m <= 2 {
        y -= 1;
        m += 12;
    }
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m - 3) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let secs = days
        .checked_mul(86400)?
        .checked_add(hour * 3600)?
        .checked_add(min * 60)?
        .checked_add(sec)?;
    secs.checked_mul(1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agenda_keeps_events_inside_the_window() {
        let ics = "\
BEGIN:VCALENDAR\r\n\
BEGIN:VEVENT\r\n\
DTSTART:20261127T090000Z\r\n\
SUMMARY:Standup\r\n\
END:VEVENT\r\n\
BEGIN:VEVENT\r\n\
DTSTART:20270101T090000Z\r\n\
SUMMARY:Later\r\n\
END:VEVENT\r\n\
END:VCALENDAR\r\n";
        let now = civil_to_unix_ms(2026, 11, 27, 0, 0, 0).unwrap();
        let day = 24 * 60 * 60 * 1000;
        let items = upcoming(ics, now, 2 * day);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].summary, "Standup");
        assert!(wants_agenda("what is on the calendar tomorrow"));
        assert!(!wants_agenda("fix the parser"));
    }
}
